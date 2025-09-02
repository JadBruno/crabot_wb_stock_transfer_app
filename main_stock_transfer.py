import os
import time
import logging

from services.warehouse_processor import OneTimeTaskProcessor
from dependencies.dependencies import (api_controller,
                                        mysql_controller,
                                        cookie_jar,
                                        authorized_headers,
                                        logger)

from services.regular_task_factory import RegularTaskFactory
from utils.logger import simple_logger

def main():
        logger.info("Запускаем main")
        
        # Разовые задания
        # one_time_task_processor = OneTimeTaskProcessor(api_controller=api_controller,
        #                 db_controller=mysql_controller,
        #                 cookie_jar=cookie_jar,
        #                 headers=authorized_headers,
        #                 logger=logger)
        # one_time_task_processor.process_one_time_tasks()
        
        # Регулярные задания

        regular_task_factory = RegularTaskFactory(db_controller=mysql_controller, 
                                                  api_controller=api_controller,
                                                  cookie_jar=cookie_jar,
                                                  headers=authorized_headers,
                                                  logger=logger)
        
        office_id_list = list(regular_task_factory.db_controller.get_warehouses_with_sort_order().keys()) # Забрали список складов с сортировкой

        quota_dict = dict(regular_task_factory.get_warehouse_quotas(office_id_list)) # Забрали квоты по складам

        mysql_controller.log_warehouse_state(quota_dict) # Залогировали состояние складов по квотам

        regular_task_factory.quota_dict = quota_dict # Передали квоты в фабрику заданий

        regular_task_factory.run() # Запуск обработки регулярных заданий


if __name__ == '__main__':
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Время выполнения программы: {elapsed_time:.2f} секунд")
