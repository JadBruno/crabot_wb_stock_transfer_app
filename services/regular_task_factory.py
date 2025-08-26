from collections import defaultdict
import time
from typing import Any, Dict, Iterable, Union, Mapping, Sequence, Callable, Optional, Tuple, List
from infrastructure.db.mysql.mysql_controller import MySQLController
from infrastructure.api.sync_controller import SyncAPIController
import logging
from utils.data_formating import ( _extract_min_target_map, _region_id_to_key, \
                                  _build_region_to_warehouses, _collect_destination_warehouses_for_plan, \
                                    _collect_source_warehouses_for_article)
from models.tasks import TaskWithProducts, ProductSizeInfo, ProductToTask
import math
from itertools import islice
import json
from models.regular_tasks.regular_tasks import RegularTaskForSize, RegionStock
import math

Row = Union[Mapping[str, Any], Sequence[Any]]

class RegularTaskFactory:
    def __init__(self,
                 db_controller: MySQLController,
                 api_controller: SyncAPIController,
                 wb_content_api_key: str,
                 logger):
        self.db_controller = db_controller
        self.api_controller = api_controller
        self.wb_content_api_key = wb_content_api_key
        self.logger = logger


    # def run(self):

    #     # Получили продукты с их текущими стоками
    #     all_product_entries = self.db_controller.get_all_products_with_stocks()

    #     # Сделали коллекцию всех 

    #     product_collection = self.create_product_collection(all_product_entries=all_product_entries)

    #     transfers = self.db_controller.get_products_transfers_on_the_way()
    #     if transfers:
    #         self.merge_transfers_on_the_way(product_collection, transfers)
        
        
    #     a = 1

    # def create_product_collection(self, all_product_entries):

    #     # словарь с артикулами, внутри каждого артикула своя сущность для артикула, в ней указан wb_article_id и коллекция с размерами артикула(size_id) {1:..., 2: ...}

    #     # у размера есть артикул, айди размера, size_name (колонка size), общее количество размера на всех складах, количество по складам (реализовано в словарях)

    #     products: Dict[int, Dict[str, Any]] = {}

    #     for row in all_product_entries:
    #         # поддержка dict и tuple/list
    #         if isinstance(row, Mapping):
    #             wb_article_id = row.get("wb_article_id")
    #             warehouse_id  = row.get("warehouse_id")
    #             size_id       = row.get("size_id")
    #             qty           = row.get("qty")
    #             size_name     = row.get("size")
    #         else:
    #             wb_article_id = row[0]
    #             warehouse_id  = row[1]
    #             size_id       = row[2]
    #             # row[3] = time_end (не используем)
    #             qty           = row[4]
    #             size_name     = row[5]

    #         # простая нормализация
    #         if wb_article_id is None or warehouse_id is None or size_id is None:
    #             continue
    #         try:
    #             wb_article_id = int(wb_article_id)
    #             warehouse_id  = int(warehouse_id)
    #             size_id       = int(size_id)
    #         except ValueError:
    #             continue
    #         qty = int(qty) if qty is not None else 0
    #         size_name = str(size_name) if size_name is not None else ""

    #         # если артикула ещё нет
    #         if wb_article_id not in products:
    #             products[wb_article_id] = {
    #                 "wb_article_id": wb_article_id,
    #                 "total_qty": 0,
    #                 "sizes": {}
    #             }

    #         art = products[wb_article_id]

    #         # если размера ещё нет
    #         if size_id not in art["sizes"]:
    #             art["sizes"][size_id] = {
    #                 "wb_article_id": wb_article_id,
    #                 "size_id": size_id,
    #                 "size_name": size_name,
    #                 "total_qty": 0,
    #                 "warehouses": {}
    #             }

    #         size_node = art["sizes"][size_id]

    #         # дозаполняем name если раньше было пусто
    #         if not size_node["size_name"] and size_name:
    #             size_node["size_name"] = size_name

    #         # склады
    #         if warehouse_id not in size_node["warehouses"]:
    #             size_node["warehouses"][warehouse_id] = 0
    #         size_node["warehouses"][warehouse_id] += qty

    #         # агрегаты
    #         size_node["total_qty"] += qty
    #         art["total_qty"] += qty

    #     return products
    
    # def merge_transfers_on_the_way(self,
    #                                 products_collection: Dict[int, Dict[str, Any]],
    #                                 transfers_rows: Iterable[Row]) -> None:
    #     touched_articles = set()

    #     for r in transfers_rows:
    #         # dict или tuple/list: (wb_article_id, warehouse_from_id, warehouse_to_id, size_id, qty, created_at)
    #         if isinstance(r, Mapping):
    #             wb_article_id   = r.get("wb_article_id") or r.get("nmId")
    #             warehouse_to_id = r.get("warehouse_to_id")
    #             size_id         = r.get("size_id")
    #             qty             = r.get("qty")
    #         else:
    #             if len(r) < 6:
    #                 continue
    #             wb_article_id, _, warehouse_to_id, size_id, qty, _ = r[:6]

    #         # приведение и валидация
    #         try:
    #             wb_article_id   = int(wb_article_id)
    #             size_id         = int(size_id)
    #             warehouse_to_id = int(warehouse_to_id)
    #             qty             = int(qty) if qty is not None else 0
    #         except (TypeError, ValueError):
    #             continue
    #         if qty == 0:
    #             continue

    #         # --- артикул ---
    #         if wb_article_id not in products_collection:
    #             products_collection[wb_article_id] = {
    #                 "wb_article_id": wb_article_id,
    #                 "total_qty": 0,
    #                 "sizes": {}
    #             }
    #         art = products_collection[wb_article_id]

    #         # --- размер ---
    #         if size_id not in art["sizes"]:
    #             art["sizes"][size_id] = {
    #                 "wb_article_id": wb_article_id,
    #                 "size_id": size_id,
    #                 "size_name": "",
    #                 "total_qty": 0,
    #                 "warehouses": {}
    #             }
    #         size_node = art["sizes"][size_id]

    #         # --- склад ---
    #         if warehouse_to_id not in size_node["warehouses"]:
    #             size_node["warehouses"][warehouse_to_id] = 0
    #         size_node["warehouses"][warehouse_to_id] += qty

    #         # итоги по размеру
    #         size_node["total_qty"] += qty

    #         touched_articles.add(wb_article_id)

    #     # финальный пересчёт total по артикулу — как сумма total_qty всех размеров
    #     for art_id in touched_articles:
    #         art = products_collection.get(art_id)
    #         if not art:
    #             continue
    #         total = 0
    #         for size_node in art["sizes"].values():
    #             q = size_node.get("total_qty", 0)
    #             try:
    #                 total += int(q)
    #             except (TypeError, ValueError):
    #                 pass
    #         art["total_qty"] = total


    # def run2(self):
            
    #         # берем настройки задания
    #         task_row  = self.db_controller.get_current_regular_task()

    #         # теперь берём стоки с регионами
    #         all_product_entries = self.db_controller.get_all_products_with_stocks_with_region()

    #         product_collection = self.create_product_collection_with_regions(all_product_entries=all_product_entries)

    #         transfers = self.db_controller.get_products_transfers_on_the_way_with_region()
    #         if transfers:
    #             self.merge_transfers_on_the_way_with_region(
    #                 products_collection=product_collection,
    #                 transfers_rows=transfers)

    #         wh_rows = self.db_controller.get_warehouses_regions_map()
    #         region_to_wh = _build_region_to_warehouses(wh_rows if wh_rows else [])

    #         tasks: List[TaskWithProducts] = []
    #         plans_by_article: Dict[int, Dict[int, Dict[int, int]]] = {}  # wb -> size -> region -> qty

    #         auth_header = {'Authorization': f'{self.wb_content_api_key}'}

    #         warehouse_map_unsorted = self.db_controller.get_real_warehouses_regions_map()

    #         warehouse_map_sorted = self.sort_real_warehouse_map(warehouse_entries_from_db=warehouse_map_unsorted)

    #         all_skus_list = list(product_collection.keys())

    #         barcode_info_data = self.get_all_barcodes(auth_header=auth_header)

    #         all_barcodes = self.create_barcode_list(barcode_entries=barcode_info_data)

    #         for region_id, warehouse_id_list in warehouse_map_sorted.items():

    #             for warehouse_id in warehouse_id_list:
    
    #                 for sku_batch in self.batched(all_skus_list, 1000):

    #                     request_body = {'skus':sku_batch}
    #                     current_stock_data = self.api_controller.request(method='POST',
    #                                                                         headers=auth_header, 
    #                                                                         json=request_body,
    #                                                                         base_url='https://marketplace-api.wildberries.ru',
    #                                                                         endpoint=f'/api/v3/stocks/{warehouse_id}')

    #                     a = 1


    #         # if task_row:
    #         #     for wb_article_id in product_collection.keys():
    #         #         task, size_plan = self.build_task_for_article_to_minimums(
    #         #             task_row=task_row,
    #         #             product_collection=product_collection,
    #         #             wb_article_id=wb_article_id,
    #         #             region_to_wh=region_to_wh)
                    
    #         #         if task:
    #         #             tasks.append(task)
    #         #             plans_by_article[int(wb_article_id)] = size_plan

    #         # return {
    #         #     "product_collection": product_collection,
    #         #     "tasks": tasks,
    #         #     "plans_by_article": plans_by_article}
            

    # def create_product_collection_with_regions(self, all_product_entries: Iterable[Row]) -> Dict[int, Dict[str, Any]]:
    #     products: Dict[int, Dict[str, Any]] = {}
    #     for row in all_product_entries:
    #         if isinstance(row, Mapping):
    #             wb_article_id = row.get("wb_article_id") or row.get("nmId")
    #             warehouse_id  = row.get("warehouse_id")
    #             size_id = row.get("size_id")
    #             qty = row.get("qty")
    #             size_name = row.get("size")
    #             region_id = row.get("region_id") or row.get("region")
    #         else:
    #             continue

    #         try:
    #             wb_article_id = int(wb_article_id)
    #             warehouse_id  = int(warehouse_id)
    #             size_id       = int(size_id)
    #         except (TypeError, ValueError):
    #             continue

    #         if region_id is None:
    #             continue
    #         try:
    #             region_id = int(region_id)
    #         except (TypeError, ValueError):
    #             continue

    #         qty = int(qty) if qty is not None else 0
    #         size_name = str(size_name) if size_name is not None else ""

    #         art = products.setdefault(wb_article_id, {"wb_article_id": wb_article_id, "total_qty": 0, "sizes": {}})
    #         size_node = art["sizes"].setdefault(size_id, {
    #             "wb_article_id": wb_article_id, "size_id": size_id, "size_name": size_name or "",
    #             "total_qty": 0, "regions": {}
    #         })
    #         if not size_node["size_name"] and size_name:
    #             size_node["size_name"] = size_name

    #         region_node = size_node["regions"].setdefault(region_id, {"region_id": region_id, "total_qty": 0, "warehouses": {}})
    #         region_node["warehouses"][warehouse_id] = region_node["warehouses"].get(warehouse_id, 0) + qty

    #         region_node["total_qty"] += qty
    #         size_node["total_qty"] += qty
    #         art["total_qty"] += qty

    #     return products

    # def merge_transfers_on_the_way_with_region(
    #     self,
    #     products_collection: Dict[int, Dict[str, Any]],
    #     transfers_rows: Iterable[Row]) -> None:

    #     for r in transfers_rows:
    #         if isinstance(r, Mapping):
    #             wb_article_id   = r.get("wb_article_id") or r.get("nmId")
    #             warehouse_to_id = r.get("warehouse_to_id")
    #             size_id = r.get("size_id")
    #             qty = r.get("qty")
    #             region_id = r.get("region_id")  # уже приходит из LEFT JOIN
    #         else:
    #             if len(r) < 7:
    #                 continue
    #             wb_article_id, _, warehouse_to_id, size_id, qty, region_id, _ = r[:7]

    #         # приведение и фильтры
    #         try:
    #             wb_article_id   = int(wb_article_id)
    #             warehouse_to_id = int(warehouse_to_id)
    #             size_id         = int(size_id)
    #             qty             = int(qty) if qty is not None else 0
    #         except (TypeError, ValueError):
    #             continue
    #         if qty == 0:
    #             continue

    #         if region_id is None:
    #             # регион обязателен
    #             continue
    #         try:
    #             region_id = int(region_id)
    #         except (TypeError, ValueError):
    #             continue

    #         # артикул 
    #         art = products_collection.setdefault(wb_article_id, {
    #             "wb_article_id": wb_article_id,
    #             "total_qty": 0,
    #             "sizes": {}
    #         })

    #         # размер
    #         size_node = art["sizes"].setdefault(size_id, {
    #             "wb_article_id": wb_article_id,
    #             "size_id": size_id,
    #             "size_name": "",
    #             "total_qty": 0,
    #             "regions": {}
    #         })

    #         # регион 
    #         region_node = size_node["regions"].setdefault(region_id, {
    #             "region_id": region_id,
    #             "total_qty": 0,
    #             "warehouses": {}
    #         })

    #         # склад назначения 
    #         region_node["warehouses"][warehouse_to_id] = region_node["warehouses"].get(warehouse_to_id, 0) + qty

    #         # агрегаты
    #         region_node["total_qty"] += qty
    #         size_node["total_qty"]   += qty
    #         art["total_qty"]         += qty


    # def plan_needed_qty_by_size_regions(self,
    #     product_collection: Dict[int, Dict[str, Any]],
    #     wb_article_id: int,
    #     min_target_map: Dict[str, Dict[str, float]]) -> Tuple[Dict[int, Dict[int, int]], Dict[int, int]]:

    #     art = product_collection.get(wb_article_id)
    #     if not art:
    #         return {}, {}

    #     size_plan_by_region: Dict[int, Dict[int, int]] = {}
    #     size_totals: Dict[int, int] = {}

    #     for size_id, size_node in art["sizes"].items():
    #         total_qty_size = int(size_node.get("total_qty", 0) or 0)
    #         if total_qty_size <= 0:
    #             continue

    #         # считаем потребность в каждом регионе
    #         plan_for_size: Dict[int, int] = {}
    #         for region_id, region_node in size_node.get("regions", {}).items():
    #             current_region_qty = int(region_node.get("total_qty", 0) or 0)
    #             region_key = _region_id_to_key(region_id)
    #             min_rate = float(min_target_map.get(region_key, {}).get("min", 0.0))
    #             min_required = int(math.ceil(min_rate * total_qty_size))
    #             need = max(0, min_required - current_region_qty)
    #             if need > 0:
    #                 plan_for_size[int(region_id)] = need

    #         if plan_for_size:
    #             size_plan_by_region[int(size_id)] = plan_for_size
    #             size_totals[int(size_id)] = sum(plan_for_size.values())

    #     return size_plan_by_region, size_totals

    # # ===== сборка TaskWithProducts =====

    # def build_task_for_article_to_minimums(
    #     self,
    #     task_row: Mapping[str, Any],
    #     product_collection: Dict[int, Dict[str, Any]],
    #     wb_article_id: int,
    #     region_to_wh: Optional[Dict[int, List[int]]] = None) -> Tuple[Optional[TaskWithProducts], Dict[int, Dict[int, int]]]:
        
    #     min_target_map = _extract_min_target_map(task_row)
    #     size_plan_by_region, size_totals = self.plan_needed_qty_by_size_regions(
    #         product_collection, wb_article_id, min_target_map
    #     )
    #     if not size_totals:
    #         return None, {}

    #     # --- сформируем назначения (to) по регионам, где есть потребность
    #     warehouses_to_ids: Optional[Dict[int, List[int]]] = None
    #     if region_to_wh:
    #         warehouses_to_ids = _collect_destination_warehouses_for_plan(size_plan_by_region, region_to_wh)

    #     # --- сформируем источники (from) где реально есть остатки по артикулу
    #     warehouses_from_ids: Optional[Dict[int, List[int]]] = _collect_source_warehouses_for_article(
    #         product_collection=product_collection,
    #         wb_article_id=int(wb_article_id),
    #     )

    #     # --- сформируем DTO продуктов
    #     products: List[ProductToTask] = []
    #     sizes_dtos: List[ProductSizeInfo] = []
    #     for size_id, total_need in size_totals.items():
    #         sizes_dtos.append(ProductSizeInfo(
    #             size_id=str(size_id),
    #             transfer_qty=total_need,
    #             transfer_qty_left_virtual=total_need,
    #             transfer_qty_left_real=total_need,
    #             is_archived=False
    #         ))
    #     products.append(ProductToTask(product_wb_id=int(wb_article_id), sizes=sizes_dtos))

    #     task = TaskWithProducts(
    #         task_id=int(task_row.get("task_id")),
    #         warehouses_from_ids=warehouses_from_ids,  
    #         warehouses_to_ids=warehouses_to_ids,     
    #         task_status=None,
    #         is_archived=False,
    #         task_creation_date=task_row.get("task_creation_date"),
    #         task_archiving_date=task_row.get("task_archiving_date"),
    #         last_change_date=task_row.get("last_change_date"),
    #         products=products
    #     )
    #     return task, size_plan_by_region
    

    # def sort_real_warehouse_map(self, warehouse_entries_from_db):

    #     warehouse_with_regions_dict = defaultdict(list)
        
    #     for entry in warehouse_entries_from_db:

    #         wh_id = entry['warehouse_id']
    #         region_id = entry['region_id']

    #         warehouse_with_regions_dict[region_id].append(wh_id)

    #     return warehouse_with_regions_dict
    
    # @staticmethod
    # def batched(iterable, n):
    #     it = iter(iterable)
    #     while batch := list(islice(it, n)):
    #         yield batch

    # def get_all_barcodes(self, auth_header: dict, all_skus_list: list) -> dict:

    #     sizes_with_stocks_dict = defaultdict(dict)
    #     barcodes_with_techsizes_and_nmid_dict = defaultdict(dict)

    #     cursor = {"limit": 100}

    #     while True:
                
    #             request_body ={"settings": {"cursor": cursor,
    #                                     "filter": {
    #                                     "withPhoto": -1}}}

    #             product_info_entry = self.api_controller.request(method='POST',
    #                                                             headers=auth_header, 
    #                                                             json=request_body,
    #                                                             base_url='https://content-api.wildberries.ru',
    #                                                             endpoint=f'/content/v2/get/cards/list')

    #             if product_info_entry.status_code == 200:
                
    #                 product_info_entry_json = product_info_entry.json()
                
    #                 cursor = product_info_entry_json['cursor']

    #                 cursor['limit'] = 100
                    
    #                 for card in product_info_entry_json['cards']:

    #                     sizes_with_stocks_dict[card['nmID']] = card['sizes']

    #                     for size_entry in card['sizes']:
    #                         for sku in size_entry['skus']:
    #                             barcodes_with_techsizes_and_nmid_dict[sku] = {'wb_article_id':card['nmID'],
    #                                                                           'techSize':size_entry['techSize'],
    #                                                                           'chrtID': size_entry['chrtID']}

    #                 x = 1

    #                 if cursor['total'] < cursor['limit']:

    #                     return barcodes_with_techsizes_and_nmid_dict
                    
                    
    #             else:
    #                 return barcodes_with_techsizes_and_nmid_dict

    #     a = 1

    # @staticmethod
    # def create_barcode_list(barcode_entries):
        
    #     all_barcodes = []
    #     for nmId_entry in barcode_entries.values():
            
    #         skus = [sku for size_entry in nmId_entry for sku in size_entry['skus']]

    #         all_barcodes += skus

    #     all_barcodes = list(set(all_barcodes))

    #     return all_barcodes
    

    # def run3(self):
            
    #         # берем настройки задания
    #         task_row  = self.db_controller.get_current_regular_task()

    #         # теперь берём стоки с регионами
    #         all_product_entries = self.db_controller.get_all_products_with_stocks_with_region()

    #         product_collection = self.create_product_collection_with_regions(all_product_entries=all_product_entries)

    #         transfers = self.db_controller.get_products_transfers_on_the_way_with_region()
    #         if transfers:
    #             self.merge_transfers_on_the_way_with_region(
    #                 products_collection=product_collection,
    #                 transfers_rows=transfers)

    #         wh_rows = self.db_controller.get_warehouses_regions_map()
    #         region_to_wh = _build_region_to_warehouses(wh_rows if wh_rows else [])

    #         tasks: List[TaskWithProducts] = []
    #         plans_by_article: Dict[int, Dict[int, Dict[int, int]]] = {}  # wb -> size -> region -> qty

    #         auth_header = {'Authorization': f'{self.wb_content_api_key}'}

    #         warehouse_map_unsorted = self.db_controller.get_real_warehouses_regions_map()

    #         warehouse_map_sorted = self.sort_real_warehouse_map(warehouse_entries_from_db=warehouse_map_unsorted)

    #         all_skus_list = list(product_collection.keys())

    #         barcode_info_data = self.get_all_barcodes(auth_header=auth_header, all_skus_list=all_skus_list)

    #         all_barcodes = tuple(set(barcode_info_data.keys()))

    #         all_stocks_by_barcode_dict = defaultdict(list)

    #         for region_id, warehouse_id_list in warehouse_map_sorted.items():

    #             for warehouse_id in warehouse_id_list:
    
    #                 for sku_batch in self.batched(all_barcodes, 1000):

    #                     request_body = {'skus':sku_batch}
    #                     current_stock_data = self.api_controller.request(method='POST',
    #                                                                         headers=auth_header, 
    #                                                                         json=request_body,
    #                                                                         base_url='https://marketplace-api.wildberries.ru',
    #                                                                         endpoint=f'/api/v3/stocks/{warehouse_id}')

    #                     for entry in current_stock_data:

    #                         all_stocks_by_barcode_dict[entry['sku']].append({
    #                             'region_id':region_id,
    #                             'warehouse_id':warehouse_id,
    #                             'amount':entry['amount']
    #                         })



    #                     a = 1



    def run4(self):
            # словари приоритетов в регионах
            region_priority_dict = self.db_controller.get_regions_with_sort_order()

            warehouse_priority_dict = self.db_controller.get_warehouses_with_sort_order()

            warehouses_available_to_stock_transfer = self.db_controller.get_office_with_regions_map()

            # берем настройки задания
            task_row  = self.db_controller.get_current_regular_task()
            # теперь берём стоки с регионами
            all_product_entries = self.db_controller.get_stocks_for_regular_tasks()
            # юху
            product_collection = self.create_product_collection_with_regions(all_product_entries=all_product_entries)

            # добавил продукты в пути

            transfers = self.db_controller.get_products_transfers_on_the_way_with_region()
            if transfers:
                self.merge_transfers_on_the_way_with_region(
                    products_collection=product_collection,
                    transfers_rows=transfers)

            tasks_for_product = self.create_task_for_product(product_collection=product_collection, 
                                                             task_row=task_row,
                                                             region_priority_dict=region_priority_dict,
                                                             warehouse_priority_dict=warehouse_priority_dict,
                                                             warehouses_available_to_stock_transfer=warehouses_available_to_stock_transfer)  

            a = 1



    def create_task_for_product(self, product_collection, task_row, region_priority_dict, warehouse_priority_dict, warehouses_available_to_stock_transfer ):
        tasks = []

        region_src_sort_order = self.sort_destinations_by_key(region_priority_dict, key='src_priority')
        region_dst_sort_order = self.sort_destinations_by_key(region_priority_dict, key='dst_priority')

        warehouse_src_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='src_priority')
        warehouse_dst_sort_order = self.sort_destinations_by_key(warehouse_priority_dict, key='dst_priority')

        for product in product_collection.values():

            source_warehouses_available_for_nmId = warehouse_src_sort_order.copy()

            source_warehouse_used_in_transfer = []

            try:
                for size_id, size_data in product["sizes"].items():
                    task_for_size = RegularTaskForSize(
                        nmId=product["wb_article_id"],
                        size=size_data["size_name"],
                        total_stock_for_product=size_data["total_qty"])

                    # Заполняем регионы
                    for region_id, region_data in size_data["regions"].items():
                        region_obj = task_for_size.region_data.get(region_id)

                        if region_obj is None:
                            continue

                        # склады
                        region_obj.warehouses = list(region_data["warehouses"].keys())
                        region_obj.stock_by_warehouse = [
                            {wh: qty} for wh, qty in region_data["warehouses"].items()]
                        
                        region_obj.stock_by_region_before = region_data["total_qty"]
                        region_obj.stock_by_region_after = region_data["total_qty"]

                        # target/min из task_row 
                        target_key = f"target_{region_obj.attribute}"
                        min_key = f"min_{region_obj.attribute}"

                        region_obj.target_share = task_row.get(target_key, 0)
                        region_obj.min_share = task_row.get(min_key, 0)

                        region_obj.target_stock_by_region = math.floor(
                            task_for_size.total_stock_for_product * region_obj.target_share)
                        
                        region_obj.min_stock_by_region = math.floor(
                            task_for_size.total_stock_for_product * region_obj.min_share)

                        region_obj.amount_to_deliver = math.floor(max(
                            0, region_obj.target_stock_by_region - region_obj.stock_by_region_before))
                        
                        region_obj.is_below_min = (region_obj.stock_by_region_before < region_obj.min_stock_by_region
                            and region_obj.amount_to_deliver > 0)
                        
                    for dst_region_id in region_dst_sort_order:
                        dst_region_data_entry = task_for_size.region_data[dst_region_id]
                        amount_to_add = dst_region_data_entry.amount_to_deliver

                        if dst_region_data_entry.is_below_min == True:
                            for src_region_id in region_src_sort_order:
                                if amount_to_add > 0:
                                    current_src_entry_warehouse_sort = [office_id for office_id in warehouse_src_sort_order if office_id in warehouses_available_to_stock_transfer[src_region_id]]

                                    src_region_data_entry = task_for_size.region_data[src_region_id]

                                    total_stock_for_region_in_allowed_warehouses = sum([qty for stock_entry in src_region_data_entry.stock_by_warehouse for office_id, qty in stock_entry.items() if office_id in current_src_entry_warehouse_sort])
                                    
                                    transferrable_amount = src_region_data_entry.stock_by_region_after - src_region_data_entry.target_stock_by_region
                                    
                                    if transferrable_amount > 0:

                                        amount_available_to_be_set = min(total_stock_for_region_in_allowed_warehouses, transferrable_amount)
                                        
                                        amount_to_be_sent = min(amount_available_to_be_set, amount_to_add)

                                        if amount_to_be_sent > 0:
                                            
                                            for office_id in current_src_entry_warehouse_sort:

                                                for wh_stock_entry in src_region_data_entry.stock_by_warehouse:
                                            
                                                    if office_id in wh_stock_entry:

                                                        qty_to_be_transferred = min(wh_stock_entry[office_id], amount_to_be_sent)

                                                        if qty_to_be_transferred:

                                                            dst_region_data_entry.stocks_to_be_sent_to_warehouse_dict[office_id] = qty_to_be_transferred

                                                            wh_stock_entry[office_id] -= qty_to_be_transferred

                                                            amount_to_add -= qty_to_be_transferred

                                                            src_region_data_entry.stock_by_region_after -= qty_to_be_transferred

                                                            amount_to_be_sent -= qty_to_be_transferred

                                                            if office_id not in source_warehouse_used_in_transfer:
                                                                source_warehouse_used_in_transfer.append(office_id)


                        a = 1



                                
                            



                    


                    
                        

                    tasks.append(task_for_size)

            except Exception as e:
                print(f"Ошибка при обработке продукта {product.get('wb_article_id')}: {e}")

        return tasks
    

    def create_product_collection_with_regions(self, all_product_entries: Iterable[Row]) -> Dict[int, Dict[str, Any]]:
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
                continue

            try:
                wb_article_id = int(wb_article_id)
                warehouse_id  = int(warehouse_id)
                size_id       = int(size_id)
            except (TypeError, ValueError):
                continue

            if region_id is None:
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
                "total_qty": 0, "regions": {}
            })
            if not size_node["size_name"] and size_name:
                size_node["size_name"] = size_name

            region_node = size_node["regions"].setdefault(region_id, {"region_id": region_id, "total_qty": 0, "warehouses": {}})
            region_node["warehouses"][warehouse_id] = region_node["warehouses"].get(warehouse_id, 0) + qty

            region_node["total_qty"] += qty
            size_node["total_qty"] += qty
            art["total_qty"] += qty

        return products

    def merge_transfers_on_the_way_with_region(
        self,
        products_collection: Dict[int, Dict[str, Any]],
        transfers_rows: Iterable[Row]) -> None:

        for r in transfers_rows:
            if isinstance(r, Mapping):
                wb_article_id   = r.get("wb_article_id") or r.get("nmId")
                warehouse_to_id = r.get("warehouse_to_id")
                size_id = r.get("size_id")
                qty = r.get("qty")
                region_id = r.get("region_id")  # уже приходит из LEFT JOIN
            else:
                if len(r) < 7:
                    continue
                wb_article_id, _, warehouse_to_id, size_id, qty, region_id, _ = r[:7]

            # приведение и фильтры
            try:
                wb_article_id   = int(wb_article_id)
                warehouse_to_id = int(warehouse_to_id)
                size_id         = int(size_id)
                qty             = int(qty) if qty is not None else 0
            except (TypeError, ValueError):
                continue
            if qty == 0:
                continue

            if region_id is None:
                # регион обязателен
                continue
            try:
                region_id = int(region_id)
            except (TypeError, ValueError):
                continue

            # артикул 
            art = products_collection.setdefault(wb_article_id, {
                "wb_article_id": wb_article_id,
                "total_qty": 0,
                "sizes": {}
            })

            # размер
            size_node = art["sizes"].setdefault(size_id, {
                "wb_article_id": wb_article_id,
                "size_id": size_id,
                "size_name": "",
                "total_qty": 0,
                "regions": {}
            })

            # регион 
            region_node = size_node["regions"].setdefault(region_id, {
                "region_id": region_id,
                "total_qty": 0,
                "warehouses": {}
            })

            # склад назначения 
            region_node["warehouses"][warehouse_to_id] = region_node["warehouses"].get(warehouse_to_id, 0) + qty

            # агрегаты
            region_node["total_qty"] += qty
            size_node["total_qty"]   += qty
            art["total_qty"]         += qty



    def sort_destinations_by_key(self, some_tuple, key):

        keys_sorted = sorted(some_tuple, key=lambda k: some_tuple[k][key])

        return keys_sorted









            