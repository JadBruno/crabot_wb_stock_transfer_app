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
        self.cooldown_error_count = 0
        # self.timeout_error_request_cooldown_list = [1, 3, 10, 30, 60, 120]
        self.timeout_error_request_cooldown_list = [60, 90, 120, 150, 180, 300, 300, 300, 300, 300]
        self.timeout_error_cooldown_index = 0
        self.send_transfer_request_cooldown = 0.1
        self.all_request_bodies_to_send = []
        self.products_with_missing_chrtids = []
        self.sent_product_queue = queue.Queue()

    @simple_logger(logger_name=__name__)
    def run_calculations(self):
            # карта размеров айди - тег
            input_data = self.load_input_data()
            size_map = input_data["size_map"] # карта размеров size: tech_size_id
            region_priority_dict = input_data["region_priority_dict"] # карта регионов {region_id: {src_priority:1, dst_priority:12}}
            warehouse_priority_dict = input_data["warehouse_priority_dict"] # карта складов {warehouse_id: {src_priority:1, dst_priority:12}}
            warehouses_available_to_stock_transfer = input_data["warehouses_available_to_stock_transfer"] # доступные склады для трансфера
            stock_availability_data = input_data["stock_availability_data"] # данные о наличии на складах
            sales_data = input_data["sales_data"] # данные о продажах
            blocked_warehouses_for_skus = input_data["blocked_warehouses_for_skus"] # заблокированные склады для товаров
            task_row = input_data["task_row"] # регуляпьная задача
            all_product_entries = input_data["all_product_entries"] # все продуктовые записи для задачи
            transfers = input_data["transfers"] # продукты в пути для задачи

            region_src_sort_order = {k: v['src_priority'] for k, v in region_priority_dict.items() if v.get('src_priority', None) is not None}

            indices = self.prepare_indices(sales_data=sales_data,
                                           stock_availability_data=stock_availability_data,
                                           last_n_days=30)

            stock_availability_df = indices["stock_availability_df"]
            orders_index = indices["orders_index"]

            product_collection = self.create_product_collection_with_regions(all_product_entries=all_product_entries,
                                                                                warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer,
                                                                                region_src_sort_order=region_src_sort_order,
                                                                                availability_index=stock_availability_df,
                                                                                orders_index=orders_index)
            # добавил продукты в пути
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

    def load_input_data(self) -> Dict[str, Any]:
        """
        Загружает все необходимые данные из DBDataFetcher для дальнейших расчётов.
        """
        result = {
            "size_map": self.size_map or self.db_data_fetcher.size_map,
            "region_priority_dict": self.db_data_fetcher.region_priority_dict,
            "warehouse_priority_dict": self.db_data_fetcher.warehouse_priority_dict,
            "warehouses_available_to_stock_transfer": self.db_data_fetcher.warehouses_available_to_stock_transfer,
            "stock_availability_data": self.db_data_fetcher.stock_availability_data,
            "sales_data": self.db_data_fetcher.sales_data,
            "blocked_warehouses_for_skus": self.db_data_fetcher.blocked_warehouses_for_skus,
            "task_row": self.db_data_fetcher.regular_task_row,
            "all_product_entries": self.db_data_fetcher.all_product_entries_for_regular_task,
            "transfers": self.db_data_fetcher.product_on_the_way_for_regular_task}
        
        return result

    def prepare_indices(self,
                        sales_data: Iterable[Mapping[str, Any]],
                        stock_availability_data: Union[Sequence[Mapping], Sequence[tuple]],
                        last_n_days: int = 30) -> Dict[str, Any]:
        """
        Строит индексы для быстрых расчётов:
        - stock_availability_df: дни наличия товара по артикулу/размеру/складу
        - orders_index: количество заказов по (nmId, techSize_id, office_id)
        """
        stock_availability_df = self.build_article_days(stock_time_data=stock_availability_data,
                                                        last_n_days=last_n_days)
                                        

        orders_index = {(row["nmId"], row["techSize_id"], row["office_id"]): row["order_count"]
                        for row in sales_data}

        result = {"stock_availability_df": stock_availability_df,
            "orders_index": orders_index}
        
        return result


    
    def build_article_days(self,
                           stock_time_data: Union[Sequence[Mapping], Sequence[tuple]],
                           last_n_days: Optional[int] = 30) -> Dict[Tuple[int, int, int], Dict[str, Any]]:
        """
        Строит индекс доступности по артикулам/размерам/складам:
        - нормализует входные данные
        - парсит даты
        - фильтрует по последним N дням
        - агрегирует по ключу (article, size, warehouse)
        """
        normalized_data = self._normalize_stock_time_data(stock_time_data)
        normalized_data = self._parse_dates(normalized_data)
        normalized_data = self._filter_recent_days(normalized_data, last_n_days)
        avail_index = self._build_availability_index(normalized_data)
        return avail_index

    def _normalize_stock_time_data(self,
                                   stock_time_data: Union[Sequence[Mapping], Sequence[tuple]]
                                    ) -> List[Dict[str, Any]]:
        """Приводим входные данные к единому виду: список словарей."""
        normalized = []
        if isinstance(stock_time_data, list) and stock_time_data:
            first = stock_time_data[0]
            if isinstance(first, dict):
                normalized = stock_time_data
            else:
                for t in stock_time_data:
                    normalized.append({
                        "wb_article_id": t[0],
                        "size_id": t[1],
                        "warehouse_id": t[2],
                        "time_beg": t[3],
                        "time_end": t[4],
                    })
        return normalized

    def _parse_dates(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Преобразуем time_beg и time_end в datetime."""
        for row in rows:
            if not isinstance(row["time_beg"], datetime):
                row["time_beg"] = datetime.fromisoformat(str(row["time_beg"]))
            if not isinstance(row["time_end"], datetime):
                row["time_end"] = datetime.fromisoformat(str(row["time_end"]))
        return rows

    def _filter_recent_days(self,
                            rows: List[Dict[str, Any]],
                            last_n_days: Optional[int]
                            ) -> List[Dict[str, Any]]:
        """Фильтруем записи по последним N дням."""
        if last_n_days is None:
            return rows
        cutoff = datetime.now() - timedelta(days=last_n_days)
        return [r for r in rows if r["time_end"] >= cutoff]
    

    def _build_availability_index(self,
                                  rows: List[Dict[str, Any]]
                                  ) -> Dict[Tuple[int, int, int], Dict[str, Any]]:
        """Строим индекс доступности по ключу (article, size, warehouse)."""
        avail_index: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
        for row in rows:
            key = (int(row["wb_article_id"]), int(row["size_id"]), int(row["warehouse_id"]))
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
        """
        # 1. Сортировки
        region_src_sort_order, region_dst_sort_order, \
        warehouse_src_sort_order, warehouse_dst_sort_order = self._prepare_sort_orders(region_priority_dict, 
                                                                                       warehouse_priority_dict
)
        self.warehouse_dst_sort_order = warehouse_dst_sort_order

        # 2. Квоты
        src_quota_by_region, dst_quota_by_region, \
        src_warehouses_with_quota_dict, dst_warehouses_with_quota_dict, \
        warehouses_with_regions = self._prepare_quota_maps(quota_dict, warehouse_src_sort_order, 
                                                           warehouse_dst_sort_order, warehouses_available_to_stock_transfer)

        # 3. Обработка продуктов
        for product in product_collection.values():
            try:
                current_tasks_for_product, product_stocks_by_size_with_warehouse = self._process_single_product(product,
                                                                                                                task_row,
                                                                                                                size_map,
                                                                                                                blocked_warehouses_for_skus,
                                                                                                                region_src_sort_order,
                                                                                                                region_dst_sort_order,
                                                                                                                warehouse_src_sort_order,
                                                                                                                warehouses_available_to_stock_transfer,
                                                                                                                src_quota_by_region,
                                                                                                                dst_quota_by_region,
                                                                                                                src_warehouses_with_quota_dict,
                                                                                                                dst_warehouses_with_quota_dict)

                if current_tasks_for_product:
                    self.create_stock_transfer_task_for_product(
                        current_tasks_for_product,
                        warehouses_available_to_stock_transfer,
                        quota_dict=quota_dict,
                        warehouse_src_sort_order=warehouse_src_sort_order,
                        warehouse_dst_sort_order=warehouse_dst_sort_order,
                        size_map=size_map,
                        product_stocks_by_size_with_warehouse=product_stocks_by_size_with_warehouse,
                    )

            except Exception as e:
                self.logger.debug(f"Ошибка при обработке продукта {product.get('wb_article_id')}: {e}")

        # 4. Сортировка запросов
        self._sort_request_bodies_by_destination_priority(warehouse_dst_sort_order)


    def _prepare_sort_orders(self, region_priority_dict, warehouse_priority_dict):
        region_src_sort_order = self.sort_destinations_by_key(region_priority_dict, key='src_priority')
        region_dst_sort_order = self.sort_destinations_by_key(region_priority_dict, key='dst_priority')
        warehouse_src_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='src_priority')
        warehouse_dst_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='dst_priority')
        return region_src_sort_order, region_dst_sort_order, warehouse_src_sort_order, warehouse_dst_sort_order

    def _prepare_quota_maps(self, quota_dict, warehouse_src_sort_order, warehouse_dst_sort_order, warehouses_available_to_stock_transfer):
        warehouses_with_regions = {val: k for k, vals in warehouses_available_to_stock_transfer.items() for val in vals}

        src_warehouses_with_quota_dict = {wh_id: quota_dict.get(wh_id, {}).get('src', 0) for wh_id in warehouse_src_sort_order}
        dst_warehouses_with_quota_dict = {wh_id: quota_dict.get(wh_id, {}).get('dst', 0) for wh_id in warehouse_dst_sort_order}

        src_quota_by_region = defaultdict(int)
        dst_quota_by_region = defaultdict(int)

        for wh_id, region_id in warehouses_with_regions.items():
            src_quota_by_region[region_id] += quota_dict.get(wh_id, {}).get('src', 0) or 0
            dst_quota_by_region[region_id] += quota_dict.get(wh_id, {}).get('dst', 0) or 0

        return src_quota_by_region, dst_quota_by_region, src_warehouses_with_quota_dict, dst_warehouses_with_quota_dict, warehouses_with_regions


    def _process_single_product(self,
                                product,
                                task_row,
                                size_map,
                                blocked_warehouses_for_skus,
                                region_src_sort_order,
                                region_dst_sort_order,
                                warehouse_src_sort_order,
                                warehouses_available_to_stock_transfer,
                                src_quota_by_region,
                                dst_quota_by_region,
                                src_warehouses_with_quota_dict,
                                dst_warehouses_with_quota_dict):
        current_tasks_for_product = []
        source_warehouse_used_in_transfer = []
        product_stocks_by_size_with_warehouse = {}

        for size_id, size_data in product.get("sizes", {}).items():
            task_for_size = self._build_task_for_size(product, size_id, size_data)
            self._fill_region_attributes(task_for_size, size_data, task_row, dst_quota_by_region)

            self._distribute_to_regions(
                task_for_size,
                size_id,
                size_data,
                blocked_warehouses_for_skus,
                region_src_sort_order,
                region_dst_sort_order,
                warehouse_src_sort_order,
                warehouses_available_to_stock_transfer,
                src_quota_by_region,
                dst_quota_by_region,
                src_warehouses_with_quota_dict,
                dst_warehouses_with_quota_dict,
                product_stocks_by_size_with_warehouse,
                source_warehouse_used_in_transfer)

            if getattr(task_for_size, 'to_process', False):
                current_tasks_for_product.append(task_for_size)

        return current_tasks_for_product, product_stocks_by_size_with_warehouse


    def _build_task_for_size(self, product, size_id, size_data):
        result = RegularTaskForSize(nmId=product.get("wb_article_id"),
                                    size=size_data.get("size_name"),
                                    tech_size_id=size_id,
                                    total_stock_for_product=size_data.get("total_qty", 0),
                                    availability_days_by_warehouse=size_data.get('availability_days_by_warehouse', {}) or {},
                                    availability_days_by_region=size_data.get('availability_days_by_region', {}) or {},
                                    orders_by_warehouse=size_data.get('orders_by_warehouse', {}) or {},
                                    orders_by_region=size_data.get('orders_by_region', {}) or {})

        return result

    def _fill_region_attributes(self, task_for_size, size_data, task_row, dst_quota_by_region):
        for region_id, region_data in size_data.get("regions", {}).items():
            region_obj = task_for_size.region_data.get(region_id)
            if region_obj is None:
                continue

            wh_dict = region_data.get("warehouses", {}) or {}
            region_obj.warehouses = list(wh_dict.keys())
            region_obj.stock_by_warehouse = [{wh: qty} for wh, qty in wh_dict.items()]
            total_qty = region_data.get("total_qty", 0)
            region_obj.stock_by_region_before = total_qty
            region_obj.stock_by_region_after = total_qty

            target_key = f"target_{region_obj.attribute}"
            min_key = f"min_{region_obj.attribute}"

            region_obj.target_share = task_row.get(target_key, 0) or 0
            region_obj.min_share = task_row.get(min_key, 0) or 0
            region_obj.target_stock_by_region = math.floor(task_for_size.total_stock_for_product * region_obj.target_share)
            region_obj.min_stock_by_region = math.floor(task_for_size.total_stock_for_product * region_obj.min_share)
            region_obj.min_qty_fixed = task_row.get(f"min_qty_to_transfer_{region_obj.attribute}", 0) or 0

            region_obj.amount_to_deliver = math.floor(max(0,
                                                        region_obj.target_stock_by_region - region_obj.stock_by_region_before,
                                                        region_obj.min_qty_fixed - region_obj.stock_by_region_before))

            region_obj.is_below_min = (region_obj.stock_by_region_before <= region_obj.min_stock_by_region  
                                       and region_obj.amount_to_deliver > 0)

            if dst_quota_by_region.get(region_id, 0) > 0:
                region_obj.can_receive = True
                region_obj.skip_reason = None
            else:
                region_obj.can_receive = False
                region_obj.skip_reason = 'no_dst_quota'

    def _distribute_to_regions(self,
                                task_for_size,
                                size_id,
                                size_data,
                                blocked_warehouses_for_skus,
                                region_src_sort_order,
                                region_dst_sort_order,
                                warehouse_src_sort_order,
                                warehouses_available_to_stock_transfer,
                                src_quota_by_region,
                                dst_quota_by_region,
                                src_warehouses_with_quota_dict,
                                dst_warehouses_with_quota_dict,
                                product_stocks_by_size_with_warehouse,
                                source_warehouse_used_in_transfer):
        """
        Распределяет недостающие товары по регионам.
        """
        for dst_region_id in region_dst_sort_order:
            dst_region_data_entry = task_for_size.region_data.get(dst_region_id)
            if not dst_region_data_entry:
                continue

            # нет квот на приём
            if dst_quota_by_region.get(dst_region_id, 0) <= 0:
                continue

            amount_to_add = dst_region_data_entry.amount_to_deliver
            if not dst_region_data_entry.is_below_min or amount_to_add <= 0:
                continue

            # доступные склады-приёмщики
            dst_warehouses_for_region = [wh_id
                                        for wh_id, quota in dst_warehouses_with_quota_dict.items()
                                        if quota > 0
                                        and wh_id in (warehouses_available_to_stock_transfer.get(dst_region_id, []) or [])
                                        and wh_id not in blocked_warehouses_for_skus.get(f"{task_for_size.nmId}_{size_id}", [])]

            if not dst_warehouses_for_region:
                dst_region_data_entry.can_receive = False
                dst_region_data_entry.skip_reason = "no_dst_warehouses_in_sort_order"
                continue

            # ищем доноров
            for src_region_id in region_src_sort_order:
                if src_region_id == dst_region_id:
                    continue
                if src_quota_by_region.get(src_region_id, 0) <= 0:
                    continue
                if amount_to_add <= 0:
                    break

                donor_ok, src_region_data_entry, current_src_entry_warehouse_sort = self._find_valid_donor_region(task_for_size,
                                                                                                                src_region_id,
                                                                                                                warehouse_src_sort_order,
                                                                                                                warehouses_available_to_stock_transfer,
                                                                                                                blocked_warehouses_for_skus)

                if not donor_ok:
                    continue

                # считаем доступный объём
                transferrable_amount = max(
                    0,
                    src_region_data_entry.stock_by_region_after - src_region_data_entry.target_stock_by_region)

                total_stock_in_allowed = sum(qty for stock_entry in src_region_data_entry.stock_by_warehouse
                                                for office_id, qty in stock_entry.items()
                                                if office_id in current_src_entry_warehouse_sort)
                
                amount_available = min(total_stock_in_allowed, transferrable_amount)
                amount_to_be_sent = min(amount_available, amount_to_add)

                if amount_to_be_sent <= 0:
                    continue

                # раскладываем по складам
                used = self._distribute_to_warehouses(src_region_data_entry,
                                                    current_src_entry_warehouse_sort,
                                                    amount_to_be_sent,
                                                    dst_region_data_entry,
                                                    size_id,
                                                    product_stocks_by_size_with_warehouse,
                                                    source_warehouse_used_in_transfer,
                                                    task_for_size)

                amount_to_add -= used


    def _find_valid_donor_region(self,
                                task_for_size,
                                src_region_id,
                                warehouse_src_sort_order,
                                warehouses_available_to_stock_transfer,
                                blocked_warehouses_for_skus):
        """
        Проверяет, может ли регион быть донором.
        """
        if src_region_id not in task_for_size.region_data:
            return False, None, []

        src_region_data_entry = task_for_size.region_data[src_region_id]

        blocked_for_sku = blocked_warehouses_for_skus.get(f"{task_for_size.nmId}_{task_for_size.tech_size_id}", [])
        current_src_entry_warehouse_sort = [office_id
                                            for office_id in warehouse_src_sort_order
                                            if office_id in (warehouses_available_to_stock_transfer.get(src_region_id, []) or [])
                                            and office_id not in blocked_for_sku]


        # проверка по availability и заказам
        has_valid_donor = False
        for warehouse_id in current_src_entry_warehouse_sort:
            days_ok = (task_for_size.availability_days_by_warehouse.get(warehouse_id, 0)
                       >= self.MIN_AVAILABILITY_DAY_COUNT_FOR_TRANSFER)
            
            orders = task_for_size.orders_by_region.get(src_region_id, 0)
            orders_ok = orders is not None and (src_region_data_entry.stock_by_region_after - orders) >= 0
            if days_ok and orders_ok:
                has_valid_donor = True
                break

        return has_valid_donor, src_region_data_entry, current_src_entry_warehouse_sort

    def _distribute_to_warehouses(self,
                                    src_region_data_entry,
                                    current_src_entry_warehouse_sort,
                                    amount_to_be_sent,
                                    dst_region_data_entry,
                                    size_id,
                                    product_stocks_by_size_with_warehouse,
                                    source_warehouse_used_in_transfer,
                                    task_for_size):
        """
        Распределяет количество по складам в порядке приоритета.
        """
        sent_total = 0
        for office_id in current_src_entry_warehouse_sort:
            if amount_to_be_sent <= 0:
                break

            for wh_stock_entry in src_region_data_entry.stock_by_warehouse:
                if amount_to_be_sent <= 0:
                    break

                if office_id not in wh_stock_entry:
                    continue

                qty_in_office = wh_stock_entry[office_id]
                if not qty_in_office:
                    continue

                qty_to_transfer = min(qty_in_office, amount_to_be_sent)
                if qty_to_transfer <= 0:
                    continue

                # фиксация в product_stocks_by_size_with_warehouse
                if office_id not in product_stocks_by_size_with_warehouse:
                    product_stocks_by_size_with_warehouse[office_id] = defaultdict(dict)
                product_stocks_by_size_with_warehouse[office_id][size_id] = qty_in_office

                # фиксация для региона-приёмщика
                dst_region_data_entry.stocks_to_be_sent_to_warehouse_dict[office_id] = (dst_region_data_entry.stocks_to_be_sent_to_warehouse_dict.get(office_id, 0)
                                                                                        + qty_to_transfer)

                # обновляем остатки
                wh_stock_entry[office_id] -= qty_to_transfer
                src_region_data_entry.stock_by_region_after -= qty_to_transfer
                amount_to_be_sent -= qty_to_transfer
                sent_total += qty_to_transfer

                if office_id not in source_warehouse_used_in_transfer:
                    source_warehouse_used_in_transfer.append(office_id)

                task_for_size.to_process = True

        return sent_total

    def _sort_request_bodies_by_destination_priority(self, warehouse_dst_sort_order):
        # Сортируем массив тел заявок по приоритету склада-получателя
        def sort_key(req_data):
            dst_warehouse_id = req_data.get("dst_warehouse_id")
            if dst_warehouse_id in warehouse_dst_sort_order:
                return warehouse_dst_sort_order.index(dst_warehouse_id)
            else:
                return float("inf")  # Если склада нет в списке приоритетов, ставим его в конец

        self.all_request_bodies_to_send.sort(key=sort_key)



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
                        
                        tech_size_id_to_product_size_info = size_map.get(size_to_create, -1)

                        product_size_info = ProductSizeInfo(size_id=size_to_create,
                                                            tech_size_id=tech_size_id_to_product_size_info,
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
            self.create_stock_transfer_request(tasks_to_process=tasks_to_process, 
                                               quota_dict=quota_dict, 
                                               size_map=size_map, 
                                               product_stocks_by_size_with_warehouse=product_stocks_by_size_with_warehouse)
        

    @simple_logger(logger_name=__name__)
    def create_stock_transfer_request(self,
                                    tasks_to_process,
                                    quota_dict,
                                    size_map,
                                    product_stocks_by_size_with_warehouse,):
        """
        Создаёт заявки на трансфер по списку задач.
        """
        for task_idx, task in enumerate(tasks_to_process, start=1):
            self.logger.info("Обработка задания #%s", task_idx)

            available_from, available_to = self._check_quota_for_task(task, quota_dict)
            if not available_from or not available_to:
                self.logger.warning("Нет доступных складов по квотам для задания #%s. Пропуск.", task_idx)
                continue

            for product_idx, product in enumerate(getattr(task, "products", []), start=1):
                self.logger.info(
                    "Задание #%s: обработка продукта #%s (nmID=%s)",
                    task_idx, product_idx, getattr(product, "product_wb_id", None))

                product_stocks = self._build_product_stocks(product,
                                                            product_stocks_by_size_with_warehouse,
                                                            available_from)
                
                if not product_stocks:
                    self.logger.info("Остатков нет для nmID=%s. Пропуск.", getattr(product, "product_wb_id", None))
                    continue

                self._build_transfer_entries(product,
                                            product_stocks,
                                            available_from,
                                            available_to,
                                            quota_dict,
                                            task)
                                
    def _check_quota_for_task(self, task, quota_dict):
        try:
            available_from, available_to = self._get_available_warehouses_by_quota(
                quota_dict=quota_dict, task=task)
            
            self.logger.debug("Доступные склады-источники: %s; получатели: %s", available_from, available_to)
            return available_from, available_to
        except Exception as e:
            self.logger.exception("Ошибка при определении доступных складов: %s", e)
            return [], []
        
    @simple_logger(logger_name=__name__)
    def _get_available_warehouses_by_quota(self, quota_dict: Dict[int, Dict[str, int]], task: TaskWithProducts) -> Tuple[List[int], List[int]]:
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

        
    def _build_product_stocks(self, product, product_stocks_by_size_with_warehouse, available_from):
        """
        Собирает остатки по продукту для доступных складов.
        """
        product_stocks = {}
        chrt_id_map = self.db_data_fetcher.techsize_with_chrtid_dict

        for wh_id, wh_entry in product_stocks_by_size_with_warehouse.items():
            if wh_id not in available_from:
                continue

            banned_wh = self.db_data_fetcher.banned_warehouses_for_nmids.get(product.product_wb_id, [])
            if wh_id in banned_wh:
                continue

            wh_items = []
            for tech_size_id, qty in wh_entry.items():
                chrt_id = chrt_id_map.get(product.product_wb_id, {}).get(tech_size_id)
                if chrt_id:
                    wh_items.append({"chrtID": chrt_id, "count": qty, "techSize": tech_size_id})
                else:
                    self.logger.warning(
                        "Для nmID=%s не найден chrtID для techSize=%s",
                        product.product_wb_id, tech_size_id,
                    )
                    self.products_with_missing_chrtids.append(product.product_wb_id)

            if wh_items:
                product_stocks[wh_id] = wh_items

        return product_stocks
    


    def _build_transfer_entries(self,
                                product,
                                product_stocks,
                                available_from,
                                available_to,
                                quota_dict,
                                task):
        """
        Строит записи трансфера по складам.
        """
        for src_wid in available_from:
            if quota_dict[src_wid]["src"] < 1:
                self.logger.debug("Недостаточно квоты на складе-донора %s. Пропуск.", src_wid)
                continue

            current_req_bodies = defaultdict(dict)

            for size in getattr(product, "sizes", []):
                if size.transfer_qty_left_virtual <= 0:
                    continue

                self.create_single_size_entries(src_warehouse_id=src_wid,
                                                size=size,
                                                product_stocks=product_stocks,
                                                available_warehouses_to_ids=available_to,
                                                quota_dict=quota_dict,
                                                task=task,
                                                current_warehouse_transfer_request_bodies=current_req_bodies)

            # Финализируем заявки
            for dst_wid, wh_entries in current_req_bodies.items():
                if quota_dict[dst_wid]["dst"] < 1 or quota_dict[src_wid]["src"] < 1:
                    continue

                req_body = self.create_transfer_request_body(
                    src_warehouse_id=src_wid,
                    dst_wrh_id=dst_wid,
                    product=product,
                    warehouse_entries=wh_entries)

                req_data_entry = {
                    "src_warehouse_id": src_wid,
                    "dst_warehouse_id": dst_wid,
                    "product": product,
                    "req_body": req_body,
                    "warehouse_entries": wh_entries}
                
                self.all_request_bodies_to_send.append(req_data_entry)
                self.logger.info("Заявка подготовлена для отправки: %s", req_data_entry)








    def send_all_requests(self, quota_dict, size_map):
        """
        Отправляет все накопленные заявки на трансфер.
        """
        for idx, req_data in enumerate(self.all_request_bodies_to_send, start=1):
            try:
                if self._should_skip_request(req_data, quota_dict):
                    continue

                response = self._execute_request(req_data)
                if not response:
                    continue

                result = self._handle_response(response, req_data, quota_dict, size_map)
                if result is False:
                    self.logger.info("Прекращаем отправку заявок.")
                    break

            except Exception as e:
                self.logger.exception(
                    "Ошибка при обработке заявки #%s (src=%s dst=%s): %s",
                    idx, req_data.get("src_warehouse_id"), req_data.get("dst_warehouse_id"), e)

        self.all_request_bodies_to_send.clear()
        self.logger.info("Завершение обработки заявок на трансфер.")


    def _should_skip_request(self, req_data, quota_dict):
        src_id = req_data.get("src_warehouse_id")
        dst_id = req_data.get("dst_warehouse_id")
        src_quota = quota_dict.get(src_id, {}).get("src", 0)
        dst_quota = quota_dict.get(dst_id, {}).get("dst", 0)

        if src_quota < 1 or dst_quota < 1:
            self.logger.debug(
                "Недостаточно квот на складах. src=%s (%s), dst=%s (%s). Пропуск.",
                src_id, src_quota, dst_id, dst_quota)
            
            return True
        return False

    def _execute_request(self, req_data):
        """
        Отправляет запрос в API.
        """
        body = req_data.get("req_body")
        self.logger.debug("Отправка заявки: %s", body)

        try:
            response = self.send_transfer_request(body)
            time.sleep(self.send_transfer_request_cooldown)
            return response
        except Exception as e:
            self.logger.exception("Ошибка при отправке запроса: %s", e)
            return None

    def _handle_response(self, response, req_data, quota_dict, size_map):
        src_id = req_data["src_warehouse_id"]
        dst_id = req_data["dst_warehouse_id"]
        product = req_data["product"]
        warehouse_entries = req_data["warehouse_entries"]

        status = response.status_code
        self.logger.info("Ответ от API: status=%s для nmID=%s", status, getattr(product, "product_wb_id", None))

        if status in [200, 201, 202, 204]:
            result = self._on_successful_request(src_id, dst_id, product, warehouse_entries, quota_dict, size_map)
            return result
        elif status in [429, 500, 502, 503, 504]:
            if self.cooldown_error_count < len(self.timeout_error_request_cooldown_list):
                result = self._on_server_error()
                return result
            else:
                self.logger.error("Превышено количество ошибок сервера. Прерываем отправку.")
                return False
        elif status in [400, 403]:
            result = self._on_bad_request(src_id, dst_id, quota_dict)
        else:
            result = self._on_fatal_error(status)

    def _on_successful_request(self, src_id, dst_id, product, warehouse_entries, quota_dict, size_map):
        try:
            total_qty_sent = sum(entry["count"] for entry in warehouse_entries.values())
            if total_qty_sent <= 0:
                self.logger.warning("Получено успешное подтверждение, но количество для nmID=%s равно 0. Пропуск.", getattr(product, "product_wb_id", None))
                return
            self.bad_request_count = 0
            self.timeout_error_cooldown_index = 0
            self.logger.info("Заявка успешно отправлена: src=%s -> dst=%s nmID=%s", src_id, dst_id, product.product_wb_id)

            for size in getattr(product, "sizes", []):
                if size.size_id in warehouse_entries:
                    qty = warehouse_entries[size.size_id]["count"]
                    quota_dict[src_id]["src"] -= qty
                    quota_dict[dst_id]["dst"] -= qty
                    entry = (product.product_wb_id, qty, size_map[size.size_id], src_id, dst_id)
                    self.sent_product_queue.put(entry)

            return True

        except Exception as e:
            self.logger.exception("Ошибка в _on_successful_request: %s", e)
            return False


    def _on_server_error(self):
        if self.bad_request_count >= self.bad_request_max_count:
            self.logger.error("Превышено количество ошибок сервера. Прерываем отправку.")
            return False
        


        if self.cooldown_error_count == len(self.timeout_error_request_cooldown_list):
            return False
        
        cooldown = self.timeout_error_request_cooldown_list[self.timeout_error_cooldown_index]

        self.logger.warning("Ошибка сервера. Кулдаун %s секунд перед повтором.", cooldown)
        time.sleep(cooldown)
        self.bad_request_count += 1
        self.cooldown_error_count += 1
        self.timeout_error_cooldown_index = (self.timeout_error_cooldown_index + 1) % len(self.timeout_error_request_cooldown_list)
        return True

    def _on_bad_request(self, src_id, dst_id, quota_dict):
        if self.bad_request_count >= self.bad_request_max_count:
            self.logger.error("Превышено количество 4xx ошибок. Прерываем.")
            return False

        self.bad_request_count += 1
        self.logger.error("Ошибка 4xx. Пробуем обновить квоты.")

        for mode, wid in [("dst", dst_id), ("src", src_id)]:
            new_quota = self.fetch_quota_for_single_warehouse(office_id=wid, mode=mode)
            quota_dict[wid][mode] = new_quota

        return True

    def _on_fatal_error(self, status):
        self.logger.error("Необработанный статус ответа: %s. Прекращаем отправку.", status)
        return False

    
        

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

            # --- МОК для отладки ---
            # class MockResponse:
            #     def __init__(self, status_code=200):
            #         self.status_code = status_code
            #     def json(self):
            #         return {"mock": True, "status": self.status_code}
            #
            # self.logger.debug("МОК: заявка не отправляется, возврат фейкового ответа.")
            # return MockResponse(status_code=200)
            # ------------------------

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

            response = self.api_controller.request(base_url="https://seller-weekly-report.wildberries.ru",
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









            
