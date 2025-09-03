from collections import defaultdict
import time
from typing import Any, Dict, Iterable, Union, Mapping, Sequence, Callable, Optional, Tuple, List
from infrastructure.db.mysql.mysql_controller import MySQLController
from infrastructure.api.sync_controller import SyncAPIController

import json
import math
import pandas as pd
import sys
from utils.logger import simple_logger

class DeliveredSupplyProcessor:
    def __init__(self,
                 db_controller:MySQLController,
                 api_controller:SyncAPIController,
                 wb_analytics_api_key,
                 logger,
                 size_map:Dict[int, str]):
        self.db_controller = db_controller
        self.api_controller = api_controller
        self.wb_analytics_api_key = wb_analytics_api_key.get('API key', None)
        self.logger = logger
        self.size_map = size_map



    def process_delivered_supplies(self):
        self.logger.info("Начинаем обработку доставленных поставок")


        products_on_the_way = self.db_controller.get_products_transfers_on_the_way_dict()

        wb_supply_destinations_by_region = self.db_controller.get_wb_supply_destinations()

        if not products_on_the_way:
            self.logger.info("Нет поставок в пути для обработки")
            return
        
        self.logger.info(f"Найдено {len(products_on_the_way)} поставок в пути для обработки")

        wb_supply_data = self.fetch_wb_supply_data()

        filtered_wb_supply_data = self.remove_unwanted_entries(wb_supply_data)

        updated_product_on_the_way_entries = self.process_product_on_the_way_entries(products_on_the_way, filtered_wb_supply_data, wb_supply_destinations_by_region)

        self.db_controller.update_product_transfer_entries(updated_product_on_the_way_entries)

        a = 1

        

    def fetch_wb_supply_data(self):

        base_url = "https://seller-analytics-api.wildberries.ru"
        method = "GET"
        endpoint = "/api/v1/analytics/goods-return"
        headers = {
            'Authorization': self.wb_analytics_api_key,
            'Content-Type': 'application/json'}
        
        date_from = (pd.Timestamp.now() - pd.Timedelta(days=14)).strftime('%Y-%m-%d')
        date_to = pd.Timestamp.now().strftime('%Y-%m-%d')
        params = {
            'dateFrom': date_from,
            'dateTo': date_to}


        result = self.api_controller.request(base_url=base_url,
                                             method=method,
                                             endpoint=endpoint,
                                             headers=headers,
                                             params=params)

        if result is None:
            self.logger.error("Ошибка при получении данных по поставкам из WB Analytics API")
            return None
        if result.status_code != 200:
            self.logger.error(f"Ошибка при получении данных по поставкам из WB Analytics API. Код ответа: {result.status_code}")
            return None
        try:
            data = result.json()
            return data
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка при декодировании JSON ответа от WB Analytics API: {e}")
            return None



    def remove_unwanted_entries(self, data:dict):
        filtered_data = []
        wanted_key = 'Перемещение остатков'
        report = data.get('report', [])

        for entry in report:
            if entry.get('returnType') == wanted_key:
                filtered_data.append(entry)

        return filtered_data
    

    def process_product_on_the_way_entries(self, product_on_the_way_entries:List[Dict[str, Any]], 
                                           filtered_wb_supply_data,
                                           wb_supply_destinations_by_region: dict):

        updated_product_on_the_way_entries = []

        new_destination_list = []

        index_totals = defaultdict(int)

        for supply_entry in filtered_wb_supply_data:

            nmId = supply_entry.get('nmId', None)
            tech_size = supply_entry.get('techSize', None)
            if tech_size:
                size_id = self.size_map.get(tech_size)
            date = supply_entry.get('orderDt', None)
            dst_office_id = supply_entry.get('dstOfficeId', None)
            status = supply_entry.get('status', None)
            dst_address = supply_entry.get('dstOfficeAddress', None)

            if (nmId is None) or \
                (tech_size is None) or \
                (date is None) or \
                (dst_office_id is None) or \
                (status is None) or \
                (dst_address is None):

                continue

            if dst_address not in wb_supply_destinations_by_region:
                new_adderss_tuple = (dst_address,)
                new_destination_list.append(new_adderss_tuple)
                continue

            dst_region_id = wb_supply_destinations_by_region.get(dst_address)
            index = (f"{nmId}_{size_id}_rto{dst_region_id}_d{date}")

            if status == 'Готово':        
                index_totals[index] += 1

        
        for index, entry_list in product_on_the_way_entries.items():
        
            index_total = index_totals.get(index, None) # 5 / 2

            for entry in entry_list: # [3, 6]
                
                if index_total and index_total > 0:

                    qty_left_to_deliver_virt = entry['qty'] - index_total 

                    qty_left_to_deliver_real = max(0, qty_left_to_deliver_virt)
                    entry['qty_left_to_deliver'] = qty_left_to_deliver_real

                    if qty_left_to_deliver_real == 0 and entry['is_finished'] == 0 and entry['finished_at'] is None:
                        entry['is_finished'] = 1
                        entry['finished_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

                    if qty_left_to_deliver_virt < 0:
                        index_total = abs(entry['qty'] - index_total)

                    updated_product_on_the_way_entries.append(entry)

        self.db_controller.insert_new_supply_destinations(new_destination_list)

        return updated_product_on_the_way_entries

        








            