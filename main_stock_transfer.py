import os
import time
import logging

from services.warehouse_processor import OneTimeTaskProcessor
from dependencies.dependencies import (api_controller,
                                        mysql_controller,
                                        cookie_jar,
                                        authorized_headers,
                                        wb_content_api_key,
                                        logger)

from services.regular_task_factory import RegularTaskFactory

def main():
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
                                                  wb_content_api_key=wb_content_api_key,
                                                  logger=logger)

        regular_task_factory.run4()


if __name__ == '__main__':
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Время выполнения программы: {elapsed_time:.2f} секунд")
