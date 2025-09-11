from infrastructure.db.mysql.mysql_controller import MySQLController

from utils.logger import simple_logger, get_logger


class DBDataFetcher:
    def __init__(self, db_controller: MySQLController):
        self.db_controller = db_controller
        self.logger = get_logger(__name__)
        # Кешированные данные
        self.region_priority_dict = None
        self.warehouse_priority_dict = None
        self.warehouses_available_to_stock_transfer = None
        self.stock_availability_data = None
        self.one_time_tasks = None
        self.max_stock_nmId = None
        self.sales_data = None
        self.size_map = None
        self.blocked_warehouses_for_skus = None
        self.regular_task_row = None
        self.all_product_entries_for_regular_task = None
        self.product_on_the_way_for_regular_task = None
        self.all_wb_offices_with_regions_dict = None

        # Забираем все данные
        self.fetch_max_stock_nmId()
        self.fetch_one_time_tasks()
        self.fetch_priority_dicts()
        self.fetch_sales_data()
        self.fetch_regular_task()
        self.fetch_all_product_entries_for_regular_tasks()
        self.fetch_size_map()
        self.fetch_blocked_warehouses_for_skus()
        self.fetch_product_on_the_way_for_regular_task()
        self.fetch_all_wb_offices_with_regions_dict()

    @simple_logger(logger_name=__name__)
    def fetch_max_stock_nmId(self) -> int | None:
        nmId = self.db_controller.get_max_stock_article()
        self.max_stock_nmId = nmId
        return nmId

    @simple_logger(logger_name=__name__)
    def fetch_one_time_tasks(self) -> dict | None:
        one_time_tasks = self.db_controller.get_all_single_tasks_with_products_dict()
        self.one_time_tasks = one_time_tasks
        return one_time_tasks
    
    @simple_logger(logger_name=__name__)
    def fetch_priority_dicts(self) -> tuple[dict, dict, dict, dict] | None:
        try:
            self.region_priority_dict = self.db_controller.get_regions_with_sort_order()

            self.warehouse_priority_dict = self.db_controller.get_warehouses_with_sort_order()

            self.warehouses_available_to_stock_transfer = self.db_controller.get_office_with_regions_map()

            self.stock_availability_data = self.db_controller.get_stock_availability_data()

            data_to_return = (self.region_priority_dict,
                self.warehouse_priority_dict,
                self.warehouses_available_to_stock_transfer,
                self.stock_availability_data)
            return data_to_return

        except Exception as e:
            self.logger.error(f"Error fetching priority dicts: {e}")
            raise
            


    @simple_logger(logger_name=__name__)
    def fetch_sales_data(self) -> dict | None:
        try:
            sales_data = self.db_controller.get_size_sales_for_warehouse()
            self.sales_data = sales_data
            return sales_data

        except Exception as e:
            self.logger.error(f"Error fetching sales data: {e}")
            raise

    @simple_logger(logger_name=__name__)
    def fetch_regular_task(self) -> tuple | None: 
        regular_task_row = self.db_controller.get_current_regular_task()
        self.regular_task_row = regular_task_row
        return regular_task_row


    @simple_logger(logger_name=__name__)
    def fetch_all_product_entries_for_regular_tasks(self) -> tuple | None:
        all_product_entries_for_regular_task = self.db_controller.get_stocks_for_regular_tasks()
        self.all_product_entries_for_regular_task = all_product_entries_for_regular_task
        return all_product_entries_for_regular_task
    
    @simple_logger(logger_name=__name__)
    def fetch_size_map(self) -> dict | None: 
        size_map = self.db_controller.get_size_map()
        self.size_map = size_map
        return size_map
        

    @simple_logger(logger_name=__name__)
    def fetch_blocked_warehouses_for_skus(self) -> dict | None: 
        blocked_warehouses_for_skus = self.db_controller.get_blocked_warehouses_for_skus()
        self.blocked_warehouses_for_skus = blocked_warehouses_for_skus
        return blocked_warehouses_for_skus

    @simple_logger(logger_name=__name__)
    def fetch_product_on_the_way_for_regular_task(self) -> tuple | None:
        product_on_the_way_for_regular_task = self.db_controller.get_products_transfers_on_the_way_with_region()
        self.product_on_the_way_for_regular_task = product_on_the_way_for_regular_task
        return product_on_the_way_for_regular_task

    @simple_logger(logger_name=__name__)
    def fetch_all_wb_offices_with_regions_dict(self) -> dict | None:
        all_wb_offices_with_regions_dict = self.db_controller.get_all_wb_offices_with_regions()
        self.all_wb_offices_with_regions_dict = all_wb_offices_with_regions_dict
        return all_wb_offices_with_regions_dict


    