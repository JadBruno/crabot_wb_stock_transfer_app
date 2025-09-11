import os
import time
import logging
import asyncio
from datetime import datetime, timedelta
from services.warehouse_processor import OneTimeTaskProcessor
from dependencies.dependencies import (api_controller,
                                        mysql_controller,
                                        cookie_jar,
                                        authorized_headers,
                                        wb_analytics_api_key,
                                        cookie_list,
                                        logger)

from services.regular_task_factory import RegularTaskFactory
from services.delivered_supply_process import DeliveredSupplyProcessor
from utils.logger import simple_logger
from services.wb_api_data_fetcher import WBAPIDataFetcher
from services.db_data_fetcher import DBDataFetcher


def main():
        logger.info("Запускаем main")

        db_data_fetcher = DBDataFetcher(db_controller=mysql_controller)

                
        wb_api_data_fetcher = WBAPIDataFetcher(api_controller=api_controller,
                                     mysql_controller=mysql_controller,
                                     cookie_list=cookie_list,
                                     headers=authorized_headers,
                                     logger=logger)

        delivered_supply_processor = DeliveredSupplyProcessor(db_controller=mysql_controller,
                                                             api_controller=api_controller,
                                                             wb_analytics_api_key=wb_analytics_api_key,
                                                             logger=logger,
                                                             size_map=db_data_fetcher.size_map)
        
        delivered_supply_processor.process_delivered_supplies()


        one_time_task_processor = OneTimeTaskProcessor(api_controller=api_controller,
                        db_controller=mysql_controller,
                        db_data_fetcher=db_data_fetcher,
                        cookie_jar=cookie_jar,
                        headers=authorized_headers,
                        logger=logger)

        regular_task_factory = RegularTaskFactory(db_controller=mysql_controller, 
                                                  api_controller=api_controller,
                                                  db_data_fetcher=db_data_fetcher,
                                                  cookie_jar=cookie_jar,
                                                  headers=authorized_headers,
                                                  size_map=db_data_fetcher.size_map,
                                                  logger=logger,
                                                  cookie_list=cookie_list)
        
        office_id_list = wb_api_data_fetcher.fetch_warehouse_list(random_present_nmid=db_data_fetcher.max_stock_nmId) # Забрали список складов с сортировкой

        now = datetime.now()

        # if now.minute != 0 and now.second != 1:
        #         next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1) + timedelta(seconds=1)
        #         wait_seconds = (next_hour - now).total_seconds()
        #         wait_seconds += 5 # Добавляем 1 секунду для надежности
        #         logger.info(f"Ждём до {next_hour.strftime('%H:%M:%S')} ({int(wait_seconds)} сек.)")
        #         time.sleep(wait_seconds)
                
        # quota_dict = asyncio.run(wb_api_data_fetcher.fetch_quota(office_id_list=office_id_list)) 
        
        quota_dict = {507: {'src': 39835, 'dst': 0}, 117986: {'src': 46346, 'dst': 354353}, 120762: {'src': 10000, 'dst': 74184}, 2737: {'src': 24709, 'dst': 0}, 130744: {'src': 0, 'dst': 459945}, 686: {'src': 24992, 'dst': 7000}, 1733: {'src': 24340, 'dst': 0}, 206348: {'src': 10000, 'dst': 72554}, 208277: {'src': 0, 'dst': 84267}, 301760: {'src': 0, 'dst': 93503}, 301809: {'src': 0, 'dst': 493275}, 301983: {'src': 0, 'dst': 3355}}

        mysql_controller.log_warehouse_state(quota_dict) # Залогировали состояние складов по квотам

        one_time_task_processor.process_one_time_tasks(quota_dict=quota_dict, 
                                                       office_id_list=office_id_list) # Запуск обработки разовых заданий

        regular_task_factory.quota_dict = quota_dict # Передали квоты в фабрику заданий

        regular_task_factory.run() # Запуск обработки регулярных заданий

        logger.debug('Запрашиваем квоты еще разок')
        quota_dict = asyncio.run(wb_api_data_fetcher.fetch_quota(office_id_list=office_id_list)) 
        mysql_controller.log_warehouse_state(quota_dict) # Залогировали состояние складов по квотам

if __name__ == '__main__':
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()      
    elapsed_time = end_time - start_time
    logger.debug(f"Время выполнения программы: {elapsed_time:.2f} секунд")
