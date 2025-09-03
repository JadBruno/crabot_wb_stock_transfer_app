import os
import time
import logging

from services.warehouse_processor import OneTimeTaskProcessor
from dependencies.dependencies import (api_controller,
                                        mysql_controller,
                                        cookie_jar,
                                        authorized_headers,
                                        wb_analytics_api_key,
                                        logger)

from services.regular_task_factory import RegularTaskFactory
from services.delivered_supply_process import DeliveredSupplyProcessor
from utils.logger import simple_logger

def main():
        logger.info("Запускаем main")
        size_map=mysql_controller.get_size_map()

        delivered_supply_processor = DeliveredSupplyProcessor(db_controller=mysql_controller,
                                                             api_controller=api_controller,
                                                             wb_analytics_api_key=wb_analytics_api_key,
                                                             logger=logger,
                                                             size_map=size_map)
        
        delivered_supply_processor.process_delivered_supplies()

        one_time_task_processor = OneTimeTaskProcessor(api_controller=api_controller,
                        db_controller=mysql_controller,
                        cookie_jar=cookie_jar,
                        headers=authorized_headers,
                        logger=logger)

        regular_task_factory = RegularTaskFactory(db_controller=mysql_controller, 
                                                  api_controller=api_controller,
                                                  cookie_jar=cookie_jar,
                                                  headers=authorized_headers,
                                                  size_map=size_map,
                                                  logger=logger)
        
        
        office_id_list = list(regular_task_factory.db_controller.get_warehouses_with_sort_order().keys()) # Забрали список складов с сортировкой

        quota_dict = dict(regular_task_factory.get_warehouse_quotas(office_id_list)) # Забрали квоты по складам

        mysql_controller.log_warehouse_state(quota_dict) # Залогировали состояние складов по квотам

        one_time_task_processor.process_one_time_tasks(quota_dict=quota_dict) # Запуск обработки разовых заданий

        regular_task_factory.quota_dict = quota_dict # Передали квоты в фабрику заданий

        regular_task_factory.run() # Запуск обработки регулярных заданий


if __name__ == '__main__':
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Время выполнения программы: {elapsed_time:.2f} секунд")
