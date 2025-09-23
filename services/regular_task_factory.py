from collections import defaultdict
import time
from typing import Any, Dict, Iterable, Union, Mapping, Sequence, Callable, Optional, Tuple, List
from infrastructure.db.mysql.mysql_controller import MySQLController
from infrastructure.api.sync_controller import SyncAPIController
from requests.cookies import RequestsCookieJar
from utils.data_formating import ( _extract_min_target_map, _region_id_to_key, \
                                  _build_region_to_warehouses, _collect_destination_warehouses_for_plan, \
                                    _collect_source_warehouses_for_article)
from models.tasks import TaskWithProducts, ProductSizeInfo, ProductToTask
import math
from models.regular_tasks.regular_tasks import RegularTaskForSize, RegionStock
import sys
from utils.logger import simple_logger
from services.db_data_fetcher import DBDataFetcher
from datetime import datetime, timedelta
import queue

Row = Union[Mapping[str, Any], Sequence[Any]]


class RegularTaskFactory:
    def __init__(self,
                 db_controller: MySQLController,
                 api_controller: SyncAPIController,
                 db_data_fetcher: DBDataFetcher,
                 cookie_jar: RequestsCookieJar,
                 headers: Dict,
                 logger,
                 size_map: Dict[int, str],
                 cookie_list: list):
        self.db_controller = db_controller
        self.api_controller = api_controller
        self.db_data_fetcher = db_data_fetcher
        self.cookie_jar = cookie_jar
        self.headers = headers
        self.logger = logger
        self.MIN_AVAILABILITY_DAY_COUNT_FOR_TRANSFER = 14
        self.quota_dict = None
        self.size_map = size_map  # карта размеров айди - тег
        self.cookie_list = cookie_list
        self.current_cookie_index = 0
        self.bad_request_count = 0
        self.bad_request_max_count = 10
        self.timeout_error_request_cooldown_list = [1, 3, 10, 30, 60, 120]
        self.timeout_error_cooldown_index = 0
        self.send_transfer_request_cooldown = 0.1
        self.all_request_bodies_to_send = []
        self.products_with_missing_chrtids = []
        self.sent_product_queue = queue.Queue()

    @simple_logger(logger_name=__name__)
    def run_calculations(self):
            # карта размеров айди - тег
            if self.size_map is None:
                self.size_map = self.db_data_fetcher.size_map
                
            size_map = self.size_map

            # словари приоритетов в регионах
            region_priority_dict = self.db_data_fetcher.region_priority_dict

            region_src_sort_order = {k: v['src_priority'] for k, v in region_priority_dict.items() if v.get('src_priority', None) is not None}

            warehouse_priority_dict = self.db_data_fetcher.warehouse_priority_dict

            warehouses_available_to_stock_transfer = self.db_data_fetcher.warehouses_available_to_stock_transfer

            stock_availability_data = self.db_data_fetcher.stock_availability_data

            stock_availability_df = self.build_article_days(stock_time_data=stock_availability_data, last_n_days=30,
                                                            warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer)

            sales_data = self.db_data_fetcher.sales_data

            blocked_warehouses_for_skus = self.db_data_fetcher.blocked_warehouses_for_skus

            orders_index = {(row["nmId"], row["techSize_id"], row["office_id"]): row["order_count"]
                            for row in sales_data}

            # берем настройки задания
            task_row  = self.db_data_fetcher.regular_task_row
            # теперь берём стоки с регионами
            all_product_entries = self.db_data_fetcher.all_product_entries_for_regular_task
            # юху
            product_collection = self.create_product_collection_with_regions(all_product_entries=all_product_entries,
                                                                                warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer,
                                                                                region_src_sort_order=region_src_sort_order,
                                                                                availability_index=stock_availability_df,
                                                                                orders_index=orders_index)
            # добавил продукты в пути

            transfers = self.db_data_fetcher.product_on_the_way_for_regular_task
            if transfers:
                self.merge_transfers_on_the_way_with_region(
                    products_collection=product_collection,
                    transfers_rows=transfers)
                
            # max_product_id = max(product_collection, key=lambda k: product_collection[k]['total_qty'])

            office_id_list = list(warehouse_priority_dict.keys())
            
            quota_dict = self.quota_dict

            self.remove_unavailable_warehouses_from_current_session(quota_dict=quota_dict,
                                                                        region_priority_dict=region_priority_dict,
                                                                        warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer,
                                                                        warehouse_priority_dict=warehouse_priority_dict)


            tasks_for_product = self.create_task_for_product(product_collection=product_collection, 
                                                             task_row=task_row,
                                                             region_priority_dict=region_priority_dict,
                                                             warehouse_priority_dict=warehouse_priority_dict,
                                                             warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer,
                                                             quota_dict=quota_dict,
                                                             size_map=size_map,
                                                             blocked_warehouses_for_skus=blocked_warehouses_for_skus)  



    
    def build_article_days(self,
        stock_time_data: Union[Sequence[Mapping], Sequence[tuple]],
        warehouses_available_to_stock_transfer,
        last_n_days: Optional[int] = 30) -> Dict[Tuple[int, int, int], Dict[str, Any]]:

        now = datetime.now()

        # приводим входные данные к единому виду: список словарей
        normalized_data = []
        if isinstance(stock_time_data, list) and stock_time_data:
            first = stock_time_data[0]
            if isinstance(first, dict):
                normalized_data = stock_time_data
            else:
                # считаем, что это кортежи
                for t in stock_time_data:
                    normalized_data.append({
                        "wb_article_id": t[0],
                        "size_id": t[1],
                        "warehouse_id": t[2],
                        "time_beg": t[3],
                        "time_end": t[4],
                    })
        else:
            normalized_data = []

        # парсим даты
        for row in normalized_data:
            if not isinstance(row["time_beg"], datetime):
                row["time_beg"] = datetime.fromisoformat(str(row["time_beg"]))
            if not isinstance(row["time_end"], datetime):
                row["time_end"] = datetime.fromisoformat(str(row["time_end"]))

        # фильтр последних N дней
        if last_n_days is not None:
            cutoff = now - timedelta(days=last_n_days)
            normalized_data = [r for r in normalized_data if r["time_end"] >= cutoff]

        # строим индекс
        avail_index: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
        for row in normalized_data:
            key = (int(row["wb_article_id"]), int(row["size_id"]), int(row["warehouse_id"]))

            # получаем список дней между time_beg и time_end
            start = row["time_beg"].date()
            end = row["time_end"].date()
            days_set = {start + timedelta(days=i) for i in range((end - start).days + 1)}

            if key in avail_index:
                avail_index[key]["days"].update(days_set)
                avail_index[key]["days_count"] = len(avail_index[key]["days"])
            else:
                avail_index[key] = {"days": days_set, "days_count": len(days_set)}

        return avail_index


    @simple_logger(logger_name=__name__)
    def remove_unavailable_warehouses_from_current_session(self, quota_dict,
                                                           region_priority_dict,
                                                           warehouses_available_to_stock_transfer,
                                                           warehouse_priority_dict):
        
        for wid, pr in warehouse_priority_dict.items():
            q = quota_dict.get(wid, {})
            if q.get("src", None) == 0:
                pr["src_priority"] = None
            if q.get("dst", None) == 0:
                pr["dst_priority"] = None

        for region_id, wids in list(warehouses_available_to_stock_transfer.items()):
            kept = []
            src_sum = 0
            dst_sum = 0
            for wid in wids:
                q = quota_dict.get(wid, {"src": 0, "dst": 0})
                if not (q.get("src", 0) == 0 and q.get("dst", 0) == 0):
                    kept.append(wid)
                src_sum += q.get("src", 0)
                dst_sum += q.get("dst", 0)

            warehouses_available_to_stock_transfer[region_id] = kept

            if src_sum == 0 and dst_sum == 0 and region_id in region_priority_dict:
                region_priority_dict[region_id]["src_priority"] = None
                region_priority_dict[region_id]["dst_priority"] = None

    
    @simple_logger(logger_name=__name__)
    def create_task_for_product(self,
                            product_collection,
                            task_row,
                            region_priority_dict,
                            warehouse_priority_dict,
                            warehouses_available_to_stock_transfer,
                            quota_dict,
                            size_map,
                            blocked_warehouses_for_skus):
        """
        Создаёт задачи перемещения по товарам/размерам.
        - Если регион-получатель не имеет ни одного склада из warehouse_dst_sort_order,
        регистрируем can_receive = False и пропускаем его
        """

    

        # Сортировки регионов и складов (источник/приём)
        region_src_sort_order = self.sort_destinations_by_key(region_priority_dict, key='src_priority')
        region_dst_sort_order = self.sort_destinations_by_key(region_priority_dict, key='dst_priority')

        warehouse_src_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='src_priority')
        warehouse_dst_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='dst_priority')

        warehouses_with_regions = {val: k for k, vals in warehouses_available_to_stock_transfer.items() for val in vals}

        src_warehouses_with_quota_dict = defaultdict(int)
        for wh_id in warehouse_src_sort_order:
            src_warehouses_with_quota_dict[wh_id] = quota_dict.get(wh_id, {}).get('src', 0)
        
        dst_warehouses_with_quota_dict = defaultdict(int)
        for wh_id in warehouse_dst_sort_order:
            dst_warehouses_with_quota_dict[wh_id] = quota_dict.get(wh_id, {}).get('dst', 0)

        src_quota_by_region = defaultdict(int)
        for wh_id, region_id in warehouses_with_regions.items():
            src_quota_by_region[region_id] += quota_dict.get(wh_id, {}).get('src', 0) or 0

        dst_quota_by_region = defaultdict(int)
        for wh_id, region_id in warehouses_with_regions.items():
            dst_quota_by_region[region_id] += quota_dict.get(wh_id, {}).get('dst', 0) or 0

        for product in product_collection.values():

            current_tasks_for_product = []
            source_warehouses_available_for_nmId = warehouse_src_sort_order.copy()
            source_warehouse_used_in_transfer = []

            product_stocks_by_size_with_warehouse = {}

            try:
                for size_id, size_data in product.get("sizes", {}).items():

                    tech_size_id = size_data.get("size_name")

                    # product_stocks_by_size_with_warehouse[tech_size_id] = defaultdict(dict)

                    block_warehouses_for_sku_index = f"{product.get('wb_article_id')}_{size_id}"
                    blocked_src_warehouses_for_sku = blocked_warehouses_for_skus.get(block_warehouses_for_sku_index, [])

                    task_for_size = RegularTaskForSize(
                        nmId=product.get("wb_article_id"),
                        size=size_data.get("size_name"),
                        tech_size_id=size_id,
                        total_stock_for_product=size_data.get("total_qty", 0),
                        availability_days_by_warehouse=size_data.get('availability_days_by_warehouse', {}) or {},\
                        availability_days_by_region=size_data.get('availability_days_by_region', {}) or {},
                        orders_by_warehouse=size_data.get('orders_by_warehouse', {}) or {},
                        orders_by_region=size_data.get('orders_by_region', {}) or {})

                    # Заполняем регионы
                    for region_id, region_data in size_data.get("regions", {}).items():
                        region_obj = task_for_size.region_data.get(region_id)
                        if region_obj is None:
                            continue

                        wh_dict = region_data.get("warehouses", {}) or {}

                        # Список складов и стоки по складам
                        region_obj.warehouses = list(wh_dict.keys())
                        region_obj.stock_by_warehouse = [{wh: qty} for wh, qty in wh_dict.items()]

                        # До/после по региону
                        total_qty = region_data.get("total_qty", 0)
                        region_obj.stock_by_region_before = total_qty
                        region_obj.stock_by_region_after = total_qty

                        # Целевые/минимальные доли из task_row по атрибуту региона
                        target_key = f"target_{region_obj.attribute}"
                        min_key = f"min_{region_obj.attribute}"

                        region_obj.target_share = task_row.get(target_key, 0) or 0
                        region_obj.min_share = task_row.get(min_key, 0) or 0


                        # Целевые/минимальные стоки в штуках
                        region_obj.target_stock_by_region = math.floor(task_for_size.total_stock_for_product * region_obj.target_share)

                        region_obj.min_stock_by_region = math.floor(task_for_size.total_stock_for_product * region_obj.min_share)

                        region_obj.min_qty_fixed = task_row.get(f"min_qty_to_transfer_{region_obj.attribute}", 0) or 0
                        # Сколько нужно довезти до цели
                        region_obj.amount_to_deliver = math.floor(max(0, region_obj.target_stock_by_region - region_obj.stock_by_region_before, 
                                                                      region_obj.min_qty_fixed-region_obj.stock_by_region_before))

                        region_obj.is_below_min = (region_obj.stock_by_region_before <= region_obj.min_stock_by_region
                                                   and region_obj.amount_to_deliver > 0)

                        # Флаг по умолчанию: регион может принимать
                        if dst_quota_by_region.get(region_id, 0) > 0:
                            region_obj.can_receive = True
                            region_obj.skip_reason = None
                        else:
                            region_obj.can_receive = False
                            region_obj.skip_reason = 'no_dst_quota'

                    # Распределение по регионам-получателям 
                    for dst_region_id in region_dst_sort_order:
                        # Пропускаем, если региона нет в данных по этому размеру
                        if dst_region_id not in task_for_size.region_data:
                            continue

                        # Пропускаем, если нет квот по приёму
                        if dst_quota_by_region.get(dst_region_id, 0) <= 0:
                            continue

                        # Доступные к приёмке склады для региона-получателя (с учётом порядка назначения)
                        dst_warehouses_for_region = [wh_id for wh_id in dst_warehouses_with_quota_dict.keys() 
                                                     if dst_warehouses_with_quota_dict[wh_id] > 0 and wh_id in 
                                                     (warehouses_available_to_stock_transfer.get(dst_region_id, []) or []) and wh_id not in blocked_src_warehouses_for_sku]


                        dst_region_data_entry = task_for_size.region_data[dst_region_id]
                        amount_to_add = dst_region_data_entry.amount_to_deliver

                        # Если нет ни одного допустимого склада-получателя — фиксируем и пропускаем регион
                        if not dst_warehouses_for_region:
                            dst_region_data_entry.can_receive = False
                            dst_region_data_entry.skip_reason = 'no_dst_warehouses_in_sort_order'
                            continue

                        if dst_region_data_entry.is_below_min and amount_to_add > 0:
                            for src_region_id in region_src_sort_order:

                                # нельзя возить внутри одного региона
                                if src_region_id == dst_region_id:
                                    continue

                                # Пропускаем, если нет квот по донору
                                if src_quota_by_region.get(src_region_id, 0) <= 0:
                                    continue

                                if amount_to_add <= 0:
                                    break

                                # Ограничиваем список складов-источников по доступности и приоритету
                                current_src_entry_warehouse_sort = [office_id for office_id in warehouse_src_sort_order
                                    if (office_id in (warehouses_available_to_stock_transfer.get(src_region_id, []) or []) and office_id not in blocked_src_warehouses_for_sku)]

                                # Если регион-источник вообще не описан для размера — дальше смысла нет
                                if src_region_id not in task_for_size.region_data:
                                    continue

                                src_region_data_entry = task_for_size.region_data[src_region_id]

                                for wh_stock in src_region_data_entry.stock_by_warehouse:
                                    for wh_id, qty in wh_stock.items():
                                        if wh_id not in product_stocks_by_size_with_warehouse:
                                            product_stocks_by_size_with_warehouse[wh_id] = defaultdict(dict)

                                        if qty and wh_id in current_src_entry_warehouse_sort:
                                            product_stocks_by_size_with_warehouse[wh_id][tech_size_id] = qty


                                # Список складов донора с известным приоритетом
                                warehouse_for_region_list = [warehouse_id for warehouse_id in src_warehouses_with_quota_dict.keys() 
                                                             if src_warehouses_with_quota_dict[warehouse_id] > 0 and warehouse_id in (warehouses_available_to_stock_transfer.get(src_region_id, []) or []) and warehouse_id not in blocked_src_warehouses_for_sku]
                                    

                                # Проверка валидности донора (по дням наличия и заказам)
                                has_valid_donor = False
                                for warehouse_id in warehouse_for_region_list:
                                    days_ok = (task_for_size.availability_days_by_warehouse.get(warehouse_id, 0)
                                            >= self.MIN_AVAILABILITY_DAY_COUNT_FOR_TRANSFER)
                                    orders = task_for_size.orders_by_region.get(region_id, 0)
                                    # не уходим в минус по региону-источнику после списания заказов
                                    orders_ok = (orders is not None and
                                                (src_region_data_entry.stock_by_region_after - orders) >= 0)
                                    if days_ok and orders_ok:
                                        has_valid_donor = True
                                        break
                                if not has_valid_donor:
                                    continue

                                # Сколько реально есть на «разрешённых» складах региона-донора
                                total_stock_for_region_in_allowed_warehouses = sum(qty
                                    for stock_entry in src_region_data_entry.stock_by_warehouse
                                    for office_id, qty in stock_entry.items()
                                    if office_id in current_src_entry_warehouse_sort)

                                # Сколько теоретически можно вывести без просадки ниже target региона-донора
                                transferrable_amount = max(0, src_region_data_entry.stock_by_region_after - src_region_data_entry.target_stock_by_region)

                                if transferrable_amount <= 0:
                                    continue

                                amount_available_to_be_sent = min(
                                    total_stock_for_region_in_allowed_warehouses,
                                    transferrable_amount)
                                
                                amount_to_be_sent = min(amount_available_to_be_sent, amount_to_add)

                                if amount_to_be_sent <= 0:
                                    continue
                                
                                amount_to_be_sent_start = amount_to_be_sent

                                # Разложение по складам-источникам в порядке приоритета
                                for office_id in current_src_entry_warehouse_sort:
                                    if amount_to_be_sent <= 0:
                                        break

                                    for wh_stock_entry in src_region_data_entry.stock_by_warehouse:
                                        if amount_to_be_sent <= 0:
                                            break

                                        if office_id in wh_stock_entry:
                                            qty_in_office = wh_stock_entry[office_id]
                                            if not qty_in_office:
                                                continue

                                            qty_to_be_transferred = min(qty_in_office, amount_to_be_sent)
                                            if qty_to_be_transferred <= 0:
                                                continue

                                            # Фиксируем объём к отправке на склад-приёмщик (здесь сохраняем по офису-источнику)
                                            dst_region_data_entry.stocks_to_be_sent_to_warehouse_dict[office_id] = \
                                                dst_region_data_entry.stocks_to_be_sent_to_warehouse_dict.get(office_id, 0) + qty_to_be_transferred

                                            # Обновляем остатки
                                            wh_stock_entry[office_id] -= qty_to_be_transferred
                                            amount_to_add -= qty_to_be_transferred
                                            src_region_data_entry.stock_by_region_after -= qty_to_be_transferred
                                            amount_to_be_sent -= qty_to_be_transferred

                                            if office_id not in source_warehouse_used_in_transfer:
                                                source_warehouse_used_in_transfer.append(office_id)

                                            if amount_to_be_sent != amount_to_be_sent_start:
                                                task_for_size.to_process = True

                    # Если по размеру что-то набралось — добавляем в список задач для товара
                    if getattr(task_for_size, 'to_process', False):
                        current_tasks_for_product.append(task_for_size)

            except Exception as e:
                self.logger.debug(f"Ошибка при обработке продукта {product.get('wb_article_id')}: {e}")

            # Сохраняем задачи по товару
            if current_tasks_for_product:
                self.create_stock_transfer_task_for_product(
                    current_tasks_for_product,
                    warehouses_available_to_stock_transfer,
                    quota_dict=quota_dict,
                    warehouse_src_sort_order=warehouse_src_sort_order,
                    warehouse_dst_sort_order=warehouse_dst_sort_order,
                    size_map=size_map,
                    product_stocks_by_size_with_warehouse=product_stocks_by_size_with_warehouse)

        return None

    
    @simple_logger(logger_name=__name__)
    def create_product_collection_with_regions(self, all_product_entries: Iterable[Row],
                                               warehouses_available_to_stock_transfer: Dict,
                                               region_src_sort_order: Dict,
                                               availability_index: Optional[Dict[Tuple[int, int, int], Dict[str, Any]]] = None,
                                               orders_index: Optional[Dict[Tuple[int, int, int], int]] = None) -> Dict[int, Dict[str, Any]]:
        
        self.logger.debug("Старт create_product_collection_with_regions() для %s продуктовых записей", len(all_product_entries) if all_product_entries else 0)
        products: Dict[int, Dict[str, Any]] = {}
        for row in all_product_entries:
            if isinstance(row, Mapping):
                wb_article_id = row.get("wb_article_id") or row.get("nmId")
                warehouse_id  = row.get("warehouse_id")
                size_id = row.get("size_id")
                qty = row.get("qty")
                size_name = row.get("size")
                region_id = row.get("region_id") or row.get("region")
                
            else:
                self.logger.debug("Пропускаем строку с неверным форматом: %s", row)
                continue

            try:
                wb_article_id = int(wb_article_id)
                warehouse_id  = int(warehouse_id)
                size_id       = int(size_id)
            except (TypeError, ValueError):
                self.logger.debug("Пропускаем строку с неверным ID: %s", row)
                continue

            if region_id is None:
                self.logger.debug("Пропускаем строку с пустым region_id: %s", row)
                continue
            try:
                region_id = int(region_id)
            except (TypeError, ValueError):
                continue

            qty = int(qty) if qty is not None else 0
            size_name = str(size_name) if size_name is not None else ""

            art = products.setdefault(wb_article_id, {"wb_article_id": wb_article_id, "total_qty": 0, "sizes": {}})
            size_node = art["sizes"].setdefault(size_id, {
                "wb_article_id": wb_article_id, "size_id": size_id, "size_name": size_name or "",
                "total_qty": 0, "regions": {},"availability_days_by_warehouse": {}, "availability_days_by_region": {},
                "orders_by_warehouse": {}, "orders_by_region":{}
            })

            # for region_id in self.db_data_fetcher.all_wb_regions_with_office_list_dict.keys():
            #     size_node['orders_by_region'].setdefault(region_id, 0)


            if not size_node["size_name"] and size_name:
                size_node["size_name"] = size_name

            region_node = size_node["regions"].setdefault(region_id, {"region_id": region_id, "total_qty": 0, "warehouses": {}})
            region_node["warehouses"][warehouse_id] = region_node["warehouses"].get(warehouse_id, 0) + qty

            region_node["total_qty"] += qty
            size_node["total_qty"] += qty
            art["total_qty"] += qty

            # доступность по складу
            if availability_index is not None:
                info = availability_index.get((wb_article_id, size_id, warehouse_id))
                if info:
                    prev = size_node["availability_days_by_warehouse"].get(warehouse_id, 0)
                    # на случай повторов берём максимум
                    if info["days_count"] > prev:
                        size_node["availability_days_by_warehouse"][warehouse_id] = int(info["days_count"])

            # заказы 
            if orders_index is not None:
                oc = orders_index.get((wb_article_id, size_id, warehouse_id))
                if oc is not None:
                    size_node["orders_by_warehouse"][warehouse_id] = oc
                                
        self.logger.debug("Собрано %s продуктов", len(products))

        if availability_index is not None:
            for art in products.values():
                aid = art["wb_article_id"]
                for sid, size_node in art["sizes"].items():
                    for rid, rnode in size_node["regions"].items():
                        union_days = set()
                        for wh in rnode["warehouses"].keys():
                            info = availability_index.get((aid, sid, wh))
                            if info:
                                union_days |= info["days"]
                        size_node["availability_days_by_region"][rid] = len(union_days)

        self.logger.debug("Заполняем отсутствующие регионы для продуктов")
        self.fill_empty_regions_for_products_in_product_collection(products=products, 
                                                                   warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer)


        for product in products.values():
            for size in product['sizes'].values():
                for region in size['regions'].values():
                    region_id = region['region_id']
                    for wh in region['warehouses'].keys():
                        if wh not in size['orders_by_warehouse']:
                            size['orders_by_warehouse'][wh] = 0
                    total_orders_for_region = sum(size['orders_by_warehouse'][office_id] for office_id in self.db_data_fetcher.all_wb_regions_with_office_list_dict[region_id] if office_id in size['orders_by_warehouse'])

                    size['orders_by_region'][region['region_id']] = total_orders_for_region

        for product in products.values():
            for size in product['sizes'].values():
                sorted_regions = self.sort_regions(size.get('regions', {}), region_src_sort_order)
                size['regions'] = sorted_regions


        return products

    def sort_regions(self, regions: dict, sort_order: dict) -> dict:
        # Сортируем регионы по приоритету, отсутствующие в sort_order идут в конец
        sorted_regions = dict(sorted(regions.items(),
                                     key=lambda item: sort_order.get(item[0], float("inf"))))
        return sorted_regions

    @simple_logger(logger_name=__name__)
    def fill_empty_regions_for_products_in_product_collection(self, products, warehouses_available_to_stock_transfer):

        for product in products.values():
            if product['sizes']:
                for size_id, size in product['sizes'].items():
                    for region_id, warehouse_list in warehouses_available_to_stock_transfer.items():

                        if size['regions']:
                            if region_id not in size['regions']:
                                empty_region_entry = {'region_id': region_id, 'total_qty': 0, 'warehouses': {warehouse_id:0 for warehouse_id in warehouse_list}}
                                size['regions'][region_id] = empty_region_entry


    @simple_logger(logger_name=__name__)
    def merge_transfers_on_the_way_with_region(self,
                                               products_collection: Dict[int, Dict[str, Any]],
                                               transfers_rows: Iterable[Row]) -> None:

        for r in transfers_rows:

            if isinstance(r, Mapping):
                wb_article_id   = r.get("wb_article_id") or r.get("nmId")
                warehouse_from_id = r.get("warehouse_from_id")
                warehouse_to_id   = r.get("warehouse_to_id")
                size_id = r.get("size_id")
                qty = r.get("qty")
                to_region_id   = r.get("to_region_id")   or r.get("region_id")  
                from_region_id = r.get("from_region_id")
            else:
                # ожидаем не меньше 8 полей 
                if len(r) < 7:
                    continue
                # индексы под новый SELECT:
                # wb_article_id, warehouse_from_id, warehouse_to_id, size_id, qty, to_region_id, from_region_id, created_at
                wb_article_id, warehouse_from_id, warehouse_to_id, size_id, qty, to_region_id, from_region_id = r[:7]

            # приведение и фильтры
            try:
                wb_article_id     = int(wb_article_id)
                warehouse_to_id   = int(warehouse_to_id)
                warehouse_from_id = int(warehouse_from_id)
                size_id           = int(size_id)
                qty               = int(qty) if qty is not None else 0
            except (TypeError, ValueError):
                continue
            if qty == 0:
                continue

            # регионы обязательны
            try:
                to_region_id   = int(to_region_id)
                from_region_id = int(from_region_id)
            except (TypeError, ValueError):
                continue

            if wb_article_id == 25196297 or size_id == 4:
                a = 1

            # получаем/создаём артикул и размер
            art = products_collection.setdefault(wb_article_id, {
                "wb_article_id": wb_article_id,
                "total_qty": 0,
                "sizes": {}
            })
            size_node = art["sizes"].setdefault(size_id, {
                "wb_article_id": wb_article_id,
                "size_id": size_id,
                "size_name": "",
                "total_qty": 0,
                "regions": {}
            })

            # ПЛЮС к складу и региону назначения ----
            to_region_node = size_node["regions"].setdefault(to_region_id, {
                "region_id": to_region_id,
                "total_qty": 0,
                "warehouses": {}})
            
            to_region_node["warehouses"][warehouse_to_id] = to_region_node["warehouses"].get(warehouse_to_id, 0) + qty
            to_region_node["total_qty"] += qty
            size_node["total_qty"] += qty
            art["total_qty"] += qty

            # МИНУС со склада и региона отправителя ----
            from_region_node = size_node["regions"].setdefault(from_region_id, {
                "region_id": from_region_id,
                "total_qty": 0,
                "warehouses": {}})

            current_wh_qty = from_region_node["warehouses"].get(warehouse_from_id, 0)
            delta = min(qty, current_wh_qty)  # чтобы не уйти в минус на уровне склада

            # уменьшаем склад
            new_wh_qty = current_wh_qty - delta
            if new_wh_qty > 0:
                from_region_node["warehouses"][warehouse_from_id] = new_wh_qty
            else:
                # обнуляем/удаляем ключ склада, если стало 0
                if warehouse_from_id in from_region_node["warehouses"]:
                    del from_region_node["warehouses"][warehouse_from_id]

            # уменьшаем агрегаты по региону/размеру/артикулу в пределах доступного delta
            from_region_node["total_qty"] = max(0, from_region_node["total_qty"] - delta)
            size_node["total_qty"]        = max(0, size_node["total_qty"] - delta)
            



    @simple_logger(logger_name=__name__)
    def sort_destinations_by_key(self, some_tuple, key):
        # Берём только те id, у которых значение не None (Не правильная логика, нам нужно брать все доступные склады)
        # filtered = {k: v for k, v in some_tuple.items() if v.get(key) is not None}
        # keys_sorted = sorted(filtered, key=lambda k: filtered[k][key])
        # return keys_sorted
        # Сортирует все значения, присваивая значениям None предварительно 999999, чтобы они оказались в конце отсортированного списка, 
        # но тоже учитывались при перераспределении остатков
        def sort_key(item):
            value = some_tuple[item].get(key)
            if value is None:
                return (1, 999999)  # None значения идут после
            else:
                return (0, value)   # Обычные значения идут сначала
    
        return sorted(some_tuple.keys(), key=sort_key)

    @simple_logger(logger_name=__name__)
    def create_stock_transfer_task_for_product(self, task_collection, 
                                               warehouses_available_to_stock_transfer, 
                                               warehouse_src_sort_order,
                                               warehouse_dst_sort_order,
                                               quota_dict, 
                                               size_map,
                                               product_stocks_by_size_with_warehouse):

        tasks_to_process = []
        
        all_transfer_entries = defaultdict(lambda: defaultdict(list))

        for task in task_collection:

            for region in task.region_data.values():

                transfer_data = region.stocks_to_be_sent_to_warehouse_dict
                
                if transfer_data:

                    for office_id in transfer_data:

                        to_be_sent_dict = {'region_id':region.id, 
                                           'qty':transfer_data[office_id]}

                        all_transfer_entries[office_id][task.size].append(to_be_sent_dict) 

        for warehouse_from_id, transfer_entry_for_size in all_transfer_entries.items():

            max_size, entry = max(((key, entry) for key, entries in transfer_entry_for_size.items() for entry in entries),
                                                            key=lambda x: x[1]["qty"])

            region_id_to_transfer_task = entry['region_id']

            warehouse_to_ids = [warehouse_id for warehouse_id in warehouses_available_to_stock_transfer[region_id_to_transfer_task] if warehouse_id in warehouse_dst_sort_order]

            product_size_array = []

            for size_to_create, entries in transfer_entry_for_size.items():
                for entry in entries:
                    if region_id_to_transfer_task == entry['region_id']:
                        

                        product_size_info = ProductSizeInfo(size_id=size_to_create,
                                                            tech_size_id=task.tech_size_id,
                                                            transfer_qty=entry['qty'],
                                                            transfer_qty_left_real=entry['qty'],
                                                            transfer_qty_left_virtual=entry['qty'],
                                                            is_archived=False)
                        product_size_array.append(product_size_info)

            products_to_task = ProductToTask(product_wb_id=task.nmId,
                                              sizes=product_size_array)

            new_task = TaskWithProducts(
                task_id=0,
                warehouses_from_ids=[warehouse_from_id],
                warehouses_to_ids=warehouse_to_ids,
                task_status=1,
                products=[products_to_task],
                is_archived=0)
            
            if new_task.warehouses_from_ids and new_task.warehouses_to_ids:
            
                tasks_to_process.append(new_task)

        if tasks_to_process:
            self.create_stock_transfer_request(tasks_to_process=tasks_to_process, quota_dict=quota_dict, size_map=size_map, product_stocks_by_size_with_warehouse=product_stocks_by_size_with_warehouse)


    @simple_logger(logger_name=__name__)
    def create_stock_transfer_request(self, tasks_to_process, quota_dict, size_map, product_stocks_by_size_with_warehouse):

        # time.sleep(0.5) - Тут пауза не нужна. Она нужна, только если реально был послан запрос к WB. А многие запросы не посылаются из-за отсутствия квот. Кулдаун перенесен сразу после запроса к WB

        products_on_the_way_array = []

        for task_idx, task in enumerate(tasks_to_process, start=1):
            self.logger.info("Обработка задания #%s", task_idx)

            try:
                # Проверяем, есть ли для задания квоты на перемещение
                available_warehouses_from_ids, available_warehouses_to_ids = self.get_available_warehouses_by_quota(quota_dict=quota_dict, task=task)
                self.logger.debug("Доступные склады-источники: %s; склады-получатели: %s",
                                  available_warehouses_from_ids, available_warehouses_to_ids)
                if not available_warehouses_from_ids or not available_warehouses_to_ids:
                    self.logger.warning("Нет доступных складов по квотам. Пропускаю задание.")
                    continue

            except Exception as e:
                self.logger.exception("Ошибка при определении доступных складов: %s", e)
                continue

            # По каждому продукту в задании проводим итерацию
            for product_idx, product in enumerate(getattr(task, "products", []) , start=1):

                product_stocks = {}

                chrt_id_map = self.db_data_fetcher.techsize_with_chrtid_dict

                warehouses_available_for_product = {wh_id for wh_id in available_warehouses_from_ids 
                                                    if wh_id not in self.db_data_fetcher.banned_warehouses_for_nmids.get(product.product_wb_id, [])}

                if product.product_wb_id == 4521536:
                    a = 1


                
                
                for size in getattr(product, "sizes", []):

                    try:
                    
                        for wh_id, wh_entry in product_stocks_by_size_with_warehouse.items():
                            
                            banned_wh_list = self.db_data_fetcher.blocked_warehouses_for_skus.get(product.product_wb_id, [-1])
                            is_banned = wh_id in banned_wh_list

                            if wh_id not in available_warehouses_from_ids or is_banned:
                                continue

                            wh_items = []

                            for tech_size_id, qty in wh_entry.items():
                                chrt_id = chrt_id_map.get(product.product_wb_id,{}).get(tech_size_id, None)
                                if chrt_id:
                                    wh_items.append({'chrtID': chrt_id, 'count': qty, 'techSize': tech_size_id})
                                else:
                                    self.logger.warning("Для nmID=%s не найден chrtID для techSize=%s", product.product_wb_id, tech_size_id)
                                    self.products_with_missing_chrtids.append(product.product_wb_id)
                            
                            product_stocks[wh_id] = wh_items

                    except Exception as e:
                        self.logger.exception("Ошибка при получении стоков для продукта nmID=%s: %s", getattr(product, "product_wb_id", None), e)
                        product_stocks = None

                empty_wh_list = []
                for warehouse_id, stocks in product_stocks.items():
                    if not stocks:
                        empty_wh_list.append(warehouse_id)

                for warehouse_id in empty_wh_list:
                        del product_stocks[warehouse_id]

                
                self.logger.info("Задание #%s: обработка продукта #%s (nmID=%s)", task_idx, product_idx, getattr(product, "product_wb_id", None))
                
                # try:
                #     # Cмотрим остатки по всем складам
                #     product_stocks, status_code = self.fetch_stocks_by_nmid(nmid=product.product_wb_id,
                #                                                warehouses_in_task_list=available_warehouses_from_ids)
                #     self.logger.info(f'Было: {product_stocks} стало {product_stocks_new}')
                    
                #     self.compare_dicts(product_stocks, product_stocks_new)
                    
                #     if status_code != 200:
                #         self.logger.error("Не получилось получить актуальные стоки для nmID=%s", getattr(product, "product_wb_id", None))
                #         if status_code in (429, 500, 502, 503, 504) and self.bad_request_count < self.bad_request_max_count:
                #             self.bad_request_count += 1
                #             self.logger.error("Ошибка %s при получении стоков nmID=%s. Пропускаем продукт.",
                #                               status_code, getattr(product, "product_wb_id", None))
                #             cooldown_needed = self.timeout_error_request_cooldown_list[self.timeout_error_cooldown_index]
                #             self.logger.error("Ждём %s секунд перед следующей попыткой.", cooldown_needed)
                #             time.sleep(cooldown_needed)
                #             self.timeout_error_cooldown_index = (self.timeout_error_cooldown_index + 1) % len(self.timeout_error_request_cooldown_list)
                #             continue
                #         elif status_code not in (200,201,202,203,204):
                #             self.logger.error("Полученный ответ от ВБ не соответсвует ожидание, отключаем скрипт")
                #             sys.exit()
                #     else:
                #         self.bad_request_count = 0
                #         self.timeout_error_cooldown_index = 0
                    
                #     self.logger.debug("Стоки для nmID=%s: %s", getattr(product, "product_wb_id", None), product_stocks)

                # except Exception as e:
                #     self.logger.exception("Ошибка при получении стоков для продукта nmID=%s: %s", getattr(product, "product_wb_id", None), e)
                #     product_stocks = None

                # Если нет остатков
                if not product_stocks:
                    self.logger.info("Остатков нет для nmID=%s. Пропуск.", getattr(product, "product_wb_id", None))
                    continue

                # Для каждого склада донора из доступных
                for src_warehouse_id in available_warehouses_from_ids:
                    warehouse_quota_src = quota_dict[src_warehouse_id]['src']  # Если квота на нуле, пропускаем
                    if warehouse_quota_src < 1:
                        self.logger.debug(f"Недостаточно квоты на складе-доноре. src: {src_warehouse_id} - {warehouse_quota_src}. Пропуск.")
                        continue

                    self.logger.debug("Обработка склада-донора src_warehouse_id=%s", src_warehouse_id)
                    current_warehouse_transfer_request_bodies = defaultdict(dict)  # Записи трансфера по донору на каждое наставление

                    for size in getattr(product, "sizes", []):
                        try:
                            if size.transfer_qty_left_virtual <= 0:
                                self.logger.debug("Размер %s: transfer_qty_left_virtual<=0, пропуск", getattr(size, "size_id", None))
                                continue

                            self.logger.debug("Создание позиций для size_id=%s (осталось виртуально=%s)",
                                              getattr(size, "size_id", None), getattr(size, "transfer_qty_left_virtual", None))

                            # Заполняем записи под размер
                            self.create_single_size_entries(
                                src_warehouse_id=src_warehouse_id,
                                size=size,
                                product_stocks=product_stocks,
                                available_warehouses_to_ids=available_warehouses_to_ids,
                                quota_dict=quota_dict,
                                task=task,
                                current_warehouse_transfer_request_bodies=current_warehouse_transfer_request_bodies)
                            
                        except Exception as e:
                            self.logger.exception("Ошибка при создании записей для size_id=%s: %s", getattr(size, "size_id", None), e)

                    for dst_warehouse_id, warehouse_entries in current_warehouse_transfer_request_bodies.items():

                        dst_warehouse_quota = quota_dict[dst_warehouse_id]['dst'] # Если квота на нуле, пропускаем
                        src_warehouse_quota = quota_dict[src_warehouse_id]['src']

                        if dst_warehouse_quota is not None and src_warehouse_quota is not None \
                            and (dst_warehouse_quota < 1 or src_warehouse_quota < 1):
                            
                            self.logger.debug(f"""Недостаточно квот на одном из складов. 
                                              src: {src_warehouse_id} - {src_warehouse_quota} | 
                                              dst: {dst_warehouse_id} - {dst_warehouse_quota}""")

                            continue

                        try:
                            self.logger.debug("Формирование тела заявки: src=%s -> dst=%s; entries=%s",
                                              src_warehouse_id, dst_warehouse_id, warehouse_entries)

                            warehouse_req_body = self.create_transfer_request_body(
                                src_warehouse_id=src_warehouse_id,
                                dst_wrh_id=dst_warehouse_id,
                                product=product,
                                warehouse_entries=warehouse_entries)

                            req_data_entry = {
                                "src_warehouse_id": src_warehouse_id,
                                "dst_warehouse_id": dst_warehouse_id,
                                "product": product,
                                "req_body": warehouse_req_body,
                                "warehouse_entries": warehouse_entries}

                            self.all_request_bodies_to_send.append(req_data_entry)  # Сохраняем тело запроса в общий массив

                            self.logger.info("Заявка подготовлена для отправки: %s", req_data_entry)

                        except Exception as e:
                            self.logger.exception("Ошибка при подготовке/отправке заявки src=%s dst=%s: %s",
                                                  src_warehouse_id, dst_warehouse_id, e)


    def send_all_requests(self, quota_dict, size_map):
        products_on_the_way_array = []

        for idx, req_data in enumerate(self.all_request_bodies_to_send, start=1):
            try:
                src_quota = quota_dict.get(req_data.get("src_warehouse_id"), {}).get('src', 0)
                dst_quota = quota_dict.get(req_data.get("dst_warehouse_id"), {}).get('dst', 0)
                if src_quota < 1 or dst_quota < 1:
                    self.logger.debug(f"Недостаточно квот на складах. src: {req_data.get('src_warehouse_id')} - {src_quota}; dst: {req_data.get('dst_warehouse_id')} - {dst_quota}. Пропуск.")
                    continue

                warehouse_req_body = req_data.get("req_body")
                src_warehouse_id = req_data.get("src_warehouse_id")
                dst_warehouse_id = req_data.get("dst_warehouse_id")
                product = req_data.get("product")
                warehouse_entries = req_data.get("warehouse_entries")
                self.logger.info("Готово тело заявки для отправки: %s", warehouse_req_body)

                try:
                    self.logger.debug(f"POST: {warehouse_req_body}")
                    self.logger.debug("Отправка заявки: %s", warehouse_req_body)

                    # response = self.send_transfer_request(warehouse_req_body)
                    # time.sleep(self.send_transfer_request_cooldown)
                    # if response.status_code in [200, 201, 202, 204]:
                    class MockResponse:
                        def __init__(self, status_code):
                            self.status_code = status_code
                    response = MockResponse(200)  # Заглушка для теста
                    mock_true = True
                    if mock_true:
                        self.bad_request_count = 0
                        self.timeout_error_cooldown_index = 0
                        self.logger.info("Заявка успешно отправлена: src=%s -> dst=%s; nmID=%s",
                                        src_warehouse_id, dst_warehouse_id, getattr(product, "product_wb_id", None))
                        for size in getattr(product, "sizes", []):
                            
                            if size.size_id in warehouse_entries:
                                quota_dict[src_warehouse_id]['src'] -= warehouse_entries[size.size_id]['count']
                                quota_dict[dst_warehouse_id]['dst'] -= warehouse_entries[size.size_id]['count']

                                product_on_the_way_entry = (product.product_wb_id, warehouse_entries[size.size_id]['count'], size_map[size.size_id], src_warehouse_id, dst_warehouse_id)
                                
                                self.sent_product_queue.put(product_on_the_way_entry)
                    elif response.status_code in [429, 500, 502, 503, 504] and self.bad_request_count < self.bad_request_max_count:
                        self.logger.error("Ошибка %s при отправке заявки на трансфер nmID=%s. Пропускаем заявку.",
                                            response.status_code, getattr(product, "product_wb_id", None))
                        # При ошибках сервера и превышении лимитов делаем кулдаун и пробуем заново запросить квоты
                        cooldown_needed = self.timeout_error_request_cooldown_list[self.timeout_error_cooldown_index]
                        self.logger.debug(f"Делаем кулдаун {cooldown_needed} секунд перед следующим запросом")
                        time.sleep(cooldown_needed)
                        self.bad_request_count += 1
                        self.timeout_error_cooldown_index = (self.timeout_error_cooldown_index + 1) % len(self.timeout_error_request_cooldown_list)


                    elif self.bad_request_count < self.bad_request_max_count and response.status_code in [400, 403]:
                        self.bad_request_count += 1
                        self.logger.error("Полученный ответ от ВБ не соответсвует ожиданию, попробуем перезапросить квоты")
                        mode = 'dst'
                        new_dst_quota = self.fetch_quota_for_single_warehouse(office_id=dst_warehouse_id, mode=mode)
                        quota_dict[dst_warehouse_id][mode] = new_dst_quota
                        mode = 'src'
                        new_src_quota = self.fetch_quota_for_single_warehouse(office_id=src_warehouse_id, mode=mode)
                        quota_dict[src_warehouse_id][mode] = new_src_quota
                    else:
                        self.logger.error("Превышено количество запросов с кодом ошибки, перестаем отправлять заявки")
                        break

                except Exception as e:
                    self.logger.debug(f"Ошибка при отправке запроса: {e}")
                    self.logger.exception("Ошибка при отправке запроса: %s", e)

            except Exception as e:
                self.logger.exception("Ошибка при подготовке/отправке заявки src=%s dst=%s: %s",
                                        src_warehouse_id, dst_warehouse_id, e)
                
        self.all_request_bodies_to_send.clear()
        # try:
        #     self.all_request_bodies_to_send.clear()  # Очищаем массив после отправки всех заявок
        #     self.db_controller.insert_products_on_the_way(items=products_on_the_way_array)
        #     # self.db_controller.update_transfer_qty_from_task(task)  # Тут в БД несем задания
        #     self.logger.info("Регуларные задания: обновлены количества трансферов в БД")
        # except Exception as e:
        #     self.logger.exception("Ошибка при отправке регулярных заданий в БД: %s", e)

        self.logger.info("Завершение обработки заявок на трансфер.")

    def compare_dicts(self, dict1: dict, dict2: dict):
        # Проверяем одинаковое ли количество индексов
        keys1, keys2 = set(dict1.keys()), set(dict2.keys())
        if keys1 != keys2:
            self.logger.info("❌ Разные множества ключей:")
            self.logger.info(f"  Только в первом: {str(keys1 - keys2)}")
            self.logger.info(f"  Только во втором: {str(keys2 - keys1)}")
            return

        self.logger.info(f"✅ Количество индексов совпадает: {len(keys1)}")

        # Проверяем размеры списков внутри каждого ключа
        for key in keys1:
            len1, len2 = len(dict1[key]), len(dict2[key])
            if len1 < len2:
                self.logger.info(f"❌ По индексу {key} разное количество chrtID: было {len1}, стало {len2}")
                return
            else:
                self.logger.info(f"✅ По индексу {key} количество chrtID совпадает: {len1}")
    

        #                     self.logger.info("Готово тело заявки для отправки: %s", warehouse_req_body)
        #                     try:
        #                         self.logger.debug(f"POST: {warehouse_req_body}")
        #                         self.logger.debug("Отправка заявки: %s", warehouse_req_body)

        #                         # response = self.send_transfer_request(warehouse_req_body)
        #                         # time.sleep(self.send_transfer_request_cooldown)
        #                         # if response.status_code in [200, 201, 202, 204]:
        #                         class MockResponse:
        #                             def __init__(self, status_code):
        #                                 self.status_code = status_code
        #                         response = MockResponse(200)  # Заглушка для теста
        #                         mock_true = True
        #                         if mock_true:
        #                             self.bad_request_count = 0
        #                             self.timeout_error_cooldown_index = 0
        #                             self.logger.info("Заявка успешно отправлена: src=%s -> dst=%s; nmID=%s",
        #                                             src_warehouse_id, dst_warehouse_id, getattr(product, "product_wb_id", None))
        #                             for size in getattr(product, "sizes", []):
                                        
        #                                 if size.size_id in warehouse_entries:
        #                                     quota_dict[src_warehouse_id]['src'] -= warehouse_entries[size.size_id]['count']
        #                                     quota_dict[dst_warehouse_id]['dst'] -= warehouse_entries[size.size_id]['count']

        #                                     product_on_the_way_entry = (product.product_wb_id, warehouse_entries[size.size_id]['count'], size_map[size.size_id], src_warehouse_id, dst_warehouse_id)
                                            
        #                                     products_on_the_way_array.append(product_on_the_way_entry)
        #                         elif response.status_code in [429, 500, 502, 503, 504] and self.bad_request_count < self.bad_request_max_count:
        #                             self.logger.error("Ошибка %s при отправке заявки на трансфер nmID=%s. Пропускаем заявку.",
        #                                               response.status_code, getattr(product, "product_wb_id", None))
        #                             # При ошибках сервера и превышении лимитов делаем кулдаун и пробуем заново запросить квоты
        #                             cooldown_needed = self.timeout_error_request_cooldown_list[self.timeout_error_cooldown_index]
        #                             self.logger.debug(f"Делаем кулдаун {cooldown_needed} секунд перед следующим запросом")
        #                             time.sleep(cooldown_needed)
        #                             self.bad_request_count += 1
        #                             self.timeout_error_cooldown_index = (self.timeout_error_cooldown_index + 1) % len(self.timeout_error_request_cooldown_list)


        #                         elif self.bad_request_count < self.bad_request_max_count and response.status_code in [400, 403]:
        #                             self.bad_request_count += 1
        #                             self.logger.error("Полученный ответ от ВБ не соответсвует ожиданию, попробуем перезапросить квоты")
        #                             mode = 'dst'
        #                             new_dst_quota = self.fetch_quota_for_single_warehouse(office_id=dst_warehouse_id, mode=mode)
        #                             quota_dict[dst_warehouse_id][mode] = new_dst_quota
        #                             mode = 'src'
        #                             new_src_quota = self.fetch_quota_for_single_warehouse(office_id=src_warehouse_id, mode=mode)
        #                             quota_dict[src_warehouse_id][mode] = new_src_quota
        #                         else:
        #                             self.logger.error("Превышено количество запросов с кодом ошибки, отключаем скрипт")
        #                             sys.exit()

        #                     except Exception as e:
        #                         self.logger.debug(f"Ошибка при отправке запроса: {e}")
        #                         self.logger.exception("Ошибка при отправке запроса: %s", e)

        #                 except Exception as e:
        #                     self.logger.exception("Ошибка при подготовке/отправке заявки src=%s dst=%s: %s",
        #                                           src_warehouse_id, dst_warehouse_id, e)

        # try:
        #     self.db_controller.insert_products_on_the_way(items=products_on_the_way_array)
        #     # self.db_controller.update_transfer_qty_from_task(task)  # Тут в БД несем задания
        #     self.logger.info("Задание #%s: обновлены количества трансферов в БД", task_idx)
        # except Exception as e:
        #     self.logger.exception("Ошибка при обновлении задания #%s в БД: %s", task_idx, e)

        # self.logger.info("Завершение обработки регулярного задания()")

    @simple_logger(logger_name=__name__)
    def get_available_warehouses_by_quota(self, quota_dict: Dict[int, Dict[str, int]], task: TaskWithProducts) -> Tuple[List[int], List[int]]:
        self.logger.debug("Расчёт доступных складов по квотам")
        try:
            available_warehouses_from_ids = [wid for wid, q in quota_dict.items() if q.get('src', 0) != 0 and wid in task.warehouses_from_ids]
            available_warehouses_to_ids = [wid for wid, q in quota_dict.items() if q.get('dst', 0) != 0 and wid in task.warehouses_to_ids]
            self.logger.info("Доступно from: %s; to: %s",
                             len(available_warehouses_from_ids), len(available_warehouses_to_ids))
            return available_warehouses_from_ids, available_warehouses_to_ids
        except Exception as e:
            self.logger.exception("Ошибка в get_available_warehouses_by_quota: %s", e)
            return [], []
        

    @simple_logger(logger_name=__name__)
    def fetch_stocks_by_nmid(self, nmid: int, warehouses_in_task_list: list):
        self.logger.debug("Запрос стоков по nmID=%s для складов: %s", nmid, warehouses_in_task_list)
        try:
            cookies = self.cookie_list[self.current_cookie_index]['cookies']
            tokenv3 = self.cookie_list[self.current_cookie_index]['tokenV3']
            headers = self.headers.copy()
            headers['AuthorizeV3'] = tokenv3
            self.current_cookie_index = (self.current_cookie_index + 1) % len(self.cookie_list)
            
            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="GET",
                endpoint="/ns/shifts/analytics-back/api/v1/stocks",
                params={"nmID": str(nmid)},
                cookies=cookies,
                headers=headers)
            
            self.logger.debug("Ответ по стокам: status=%s", getattr(response, "status_code", None))

            stock_by_warehouse_dict = {}
            response_json = response.json()
            stock_data = response_json.get("data", {})
            src_data = stock_data.get("src", [])
            for warehouse in src_data:
                try:
                    office_id = warehouse["officeID"]
                    if office_id in warehouses_in_task_list:
                        stock_by_warehouse_dict[office_id] = warehouse["inStock"]
                except Exception as inner_e:
                    self.logger.exception("Ошибка парсинга склада в стоках: %s", inner_e)
                    continue

            self.logger.info("Получены стоки по nmID=%s: %s складов", nmid, len(stock_by_warehouse_dict))
            return stock_by_warehouse_dict, response.status_code

        except Exception as e:
            self.logger.exception("Ошибка в fetch_stocks_by_nmid nmID=%s: %s", nmid, e)
            return None


    @simple_logger(logger_name=__name__)
    def create_transfer_request_body(self, src_warehouse_id, dst_wrh_id, product, warehouse_entries):
        self.logger.debug("Формирование тела transfer request: src=%s dst=%s nmID=%s",
                          src_warehouse_id, dst_wrh_id, getattr(product, "product_wb_id", None))
        try:
            warehouse_req_body = {
                "order": {
                    "src": src_warehouse_id,
                    "dst": dst_wrh_id,
                    "nmID": product.product_wb_id,
                    "count": []}}

            for size_id, count_entry in warehouse_entries.items():
                warehouse_req_body['order']['count'].append(count_entry)

            self.logger.debug("Сформирован body: %s", warehouse_req_body)
            return warehouse_req_body
        
        except Exception as e:
            self.logger.exception("Ошибка при формировании тела заявки: %s", e)


    @simple_logger(logger_name=__name__)
    def create_single_size_entries(self,
                                   src_warehouse_id: int,
                                   size: ProductSizeInfo,
                                   product_stocks: dict,
                                   available_warehouses_to_ids: list,
                                   quota_dict: dict,
                                   task: TaskWithProducts,
                                   current_warehouse_transfer_request_bodies: defaultdict):
        self.logger.debug("Старт create_single_size_entries(): src=%s size_id=%s",
                          src_warehouse_id, getattr(size, "size_id", None))
        try:
            size_id = size.size_id
            size_stock_list = product_stocks.get(src_warehouse_id, [])
            self.logger.debug("size_stock_list для склада %s: %s позиций", src_warehouse_id, len(size_stock_list))

            for stock_entry in size_stock_list:
                if stock_entry['techSize'] != size_id:
                    continue

                available_qty = stock_entry['count']
                if available_qty <= 0:
                    self.logger.debug("Доступное количество 0 для size_id=%s, пропуск", size_id)
                    continue

                move_qty = min(size.transfer_qty_left_virtual, available_qty)
                if move_qty <= 0:
                    self.logger.debug("move_qty<=0 для size_id=%s, пропуск", size_id)
                    continue

                for dst_warehouse_id in task.warehouses_to_ids:
                    if size.transfer_qty_left_virtual > 0 and move_qty > 0:
                        try:
                            dst_quota = quota_dict[dst_warehouse_id]['dst']
                        except Exception as e:
                            self.logger.exception("Ошибка доступа к квоте dst для склада %s: %s", dst_warehouse_id, e)
                            dst_quota = 0

                        if dst_quota > 0 and dst_warehouse_id in available_warehouses_to_ids:
                            transfer_amount = min(dst_quota, move_qty, available_qty)
                            request_count_entry = {
                                "chrtID": stock_entry["chrtID"],
                                "count": transfer_amount}
                            
                            current_warehouse_transfer_request_bodies[dst_warehouse_id][size_id] = request_count_entry
                            self.logger.debug("Добавлена запись в запрос: dst=%s size_id=%s amount=%s",
                                              dst_warehouse_id, size_id, transfer_amount)
                            available_qty -= transfer_amount
                            size.transfer_qty_left_virtual -= transfer_amount
                            move_qty -= transfer_amount
                            
        except Exception as e:
            self.logger.exception("Ошибка в create_single_size_entries: %s", e)

    @simple_logger(logger_name=__name__)
    def send_transfer_request(self, request_body: dict):
        self.logger.debug("Отправка transfer request: %s", request_body)
        try:
            cookies = self.cookie_list[self.current_cookie_index]['cookies']
            tokenv3 = self.cookie_list[self.current_cookie_index]['tokenV3']
            headers = self.headers.copy()
            headers['AuthorizeV3'] = tokenv3
            self.current_cookie_index = (self.current_cookie_index + 1) % len(self.cookie_list) 

            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="POST",
                endpoint="/ns/shifts/analytics-back/api/v1/order",
                json=request_body,
                cookies=cookies,
                headers=headers)
            
            self.logger.info("Ответ на transfer request: status=%s", getattr(response, "status_code", None))
            return response

        except Exception as e:
            self.logger.exception("Ошибка в send_transfer_request: %s", e)
            return None


    @simple_logger(logger_name=__name__)
    def fetch_quota_for_single_warehouse(self,office_id, mode):
        try:
            cookies = self.cookie_list[self.current_cookie_index]['cookies']
            tokenv3 = self.cookie_list[self.current_cookie_index]['tokenV3']
            headers = self.headers.copy()
            headers['AuthorizeV3'] = tokenv3

            response_opt = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="OPTIONS",
                endpoint="/ns/shifts/analytics-back/api/v1/quota",
                params={"officeID": office_id, "type": mode},
                cookies=cookies,
                headers=headers)
                            
            self.logger.debug("OPTIONS квоты отправлен office_id=%s mode=%s", office_id, mode)

            if response_opt.status_code not in [200, 201, 202, 204]:
                raise RuntimeError(f"Unexpected status code on OPTIONS quota request: {response_opt.status_code}")

            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="GET",
                endpoint="/ns/shifts/analytics-back/api/v1/quota",
                params={"officeID": office_id, "type": mode},
                cookies=cookies,
                headers=headers)
            
            self.logger.debug("GET квоты получен office_id=%s mode=%s status=%s",
                                office_id, mode, getattr(response, "status_code", None))

            response_json = response.json()
            response_data = response_json.get("data", {})
            response_quota = response_data.get("quota", 0)
            self.logger.debug("Квота office_id=%s mode=%s = %s", office_id, mode, response_quota)
            return response_quota
        
        except:
            self.logger.exception("Ошибка при запросе квоты для office_id=%s mode=%s", office_id, mode)
            self.bad_request_count +=1
            return 0


    def product_on_the_way_consumer(self):
        """
        Функция-потребитель для обработки очереди отправленных продуктов.
        """
        while True:
            try:
                item = self.sent_product_queue.get(timeout=600)  # Ждем элемент с таймаутом
                if item is None:  # Проверка на сигнал завершения
                    break
                self.logger.debug(f"Запись отправленного продукта в БД: {item}")
                items = [item]
                self.db_controller.insert_products_on_the_way(items=items)
                self.sent_product_queue.task_done()
                self.logger.info(f"Продукт успешно добавлен в БД: {item}")
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.exception("Ошибка при обработке элемента очереди: %s", e)




"""
{"order":
    {"src":206348,"dst":120762,"nmID":456709413,
    "count":[{"chrtID":644101681,"count":1},
    {"chrtID":644101683,"count":1}]}}


"""











            
