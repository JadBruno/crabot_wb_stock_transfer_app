from collections import defaultdict
from infrastructure.db.mysql.base import SyncDatabase
from models.tasks import TaskWithProducts, ProductToTask, ProductSizeInfo
import json
from utils.logger import simple_logger

class MySQLController():

    def __init__(self, db:SyncDatabase):

        self.db = db


    @staticmethod
    def _parse_json_field(value) -> tuple:
        if value is None:
            return ()
        if isinstance(value, str):
            return tuple(json.loads(value))
        if isinstance(value, list):
            return tuple(value)
        return (value,)


    def get_cookies_by_id(self, record_id: int) -> str | None:
        query = """
            SELECT cookies 
            FROM dostup.wb_stock_transfer_data 
            WHERE id = %s
        """
        result = self.db.execute_query(query, (record_id,))
        if result:
            return result[0]["cookies"]
        return None


    @simple_logger(logger_name=__name__)
    def get_all_tasks_with_products_dict(self) -> dict[int, TaskWithProducts]:
        # Получаем все задания
        tasks_query = """SELECT 
                            task_id,
                            warehouses_from_ids,
                            warehouses_to_ids,
                            task_status,
                            is_archived,
                            task_creation_date,
                            task_archiving_date,
                            last_change_date
                        FROM mp_data.a_wb_stock_transfer_one_time_tasks
                        WHERE is_archived != 1
                        AND warehouses_from_ids IS NOT NULL
                        AND warehouses_to_ids IS NOT NULL
                        AND task_status IS NOT NULL
                        AND task_status IN (0, 1);
                    """
        tasks = self.db.execute_query(tasks_query)

        # Получаем все продукты
        products_query = """SELECT 
                                task_id,
                                product_wb_id,
                                size_id,
                                transfer_qty,
                                transfer_qty_left,
                                is_archived
                            FROM mp_data.a_wb_stock_transfer_products_to_one_time_tasks
                            WHERE task_id IS NOT NULL
                            AND product_wb_id IS NOT NULL
                            AND size_id IS NOT NULL
                            AND transfer_qty IS NOT NULL
                            AND transfer_qty_left IS NOT NULL;
                        """
        products = self.db.execute_query(products_query)

        # Группировка продуктов по task_id и product_wb_id
        grouped_products: dict[int, dict[int, list[ProductSizeInfo]]] = defaultdict(lambda: defaultdict(list))

        for p in products:
            task_id = p["task_id"]
            product_wb_id = p["product_wb_id"]

            size_info = ProductSizeInfo(
                size_id=str(p["size_id"]),
                transfer_qty=p["transfer_qty"],
                transfer_qty_left_virtual=p["transfer_qty_left"],
                transfer_qty_left_real=p["transfer_qty_left"],
                is_archived=bool(p.get("is_archived", False)))

            grouped_products[task_id][product_wb_id].append(size_info)

        # Формируем итоговый словарь
        result: dict[int, TaskWithProducts] = {}

        for t in tasks:
            task_id = t["task_id"]
            task_products_data = grouped_products.get(task_id, {})

            # Преобразуем данные о продуктах в список ProductToTask
            product_models = [
                ProductToTask(product_wb_id=product_id, sizes=sizes)
                for product_id, sizes in task_products_data.items()
            ]

            result[task_id] = TaskWithProducts(
                task_id=task_id,
                warehouses_from_ids=self._parse_json_field(t["warehouses_from_ids"]),
                warehouses_to_ids=self._parse_json_field(t["warehouses_to_ids"]),
                task_status=t["task_status"],
                is_archived=bool(t["is_archived"]),
                task_creation_date=t["task_creation_date"],
                task_archiving_date=t["task_archiving_date"],
                last_change_date=t["last_change_date"],
                products=product_models
            )

        return result
    
    @simple_logger(logger_name=__name__)
    def update_transfer_qty_from_task(self, task: TaskWithProducts) -> int:
        if not task or not task.products:
            return False

        try:
            # 1. Обновляем transfer_qty_left по каждой записи
            sql = f"""UPDATE mp_data.a_wb_stock_transfer_products_to_one_time_tasks
                        SET transfer_qty_left = %s
                        WHERE task_id = %s
                        AND product_wb_id = %s
                        AND size_id = %s;"""

            params: list[tuple[int, int, int, int]] = []
            for p in task.products:
                for s in p.sizes:
                    params.append((
                        int(s.transfer_qty_left_real),
                        int(task.task_id),
                        int(p.product_wb_id),
                        int(s.size_id),
                    ))

            self.db.execute_many(sql, params)

            # Проверяем завершено ли задание
            all_zero = all(int(s.transfer_qty_left_real) == 0 for p in task.products for s in p.sizes)

            # Завершаем
            if all_zero:
                sql_update_status = """UPDATE mp_data.a_wb_stock_transfer_one_time_tasks
                                    SET task_status = 2
                                    WHERE task_id = %s;"""
                self.db.execute_non_query(sql_update_status, (int(task.task_id),))
            else:
                if task.task_status == 0:
                    sql_update_status = """UPDATE mp_data.a_wb_stock_transfer_one_time_tasks
                                        SET task_status = 1
                                        WHERE task_id = %s;"""
                    self.db.execute_non_query(sql_update_status, (int(task.task_id),))

            return True

        except Exception as e:
            
            return False

    @simple_logger(logger_name=__name__)
    def get_max_stock_article(self) -> tuple[int, int] | None:
        try:
            sql = """
                SELECT wb_article_id, MAX(qty) AS max_qty
                FROM mp_data.a_wb_catalog_stocks
                WHERE time_beg > NOW() - INTERVAL 48 HOUR
                GROUP BY wb_article_id
                ORDER BY max_qty DESC
                LIMIT 1;
            """
            result = self.db.execute_query(sql)

            if result:
                wb_article_id = result[0]['wb_article_id']
                return wb_article_id
            else:
                return None

        except Exception as e:
            
            return None
        

    @simple_logger(logger_name=__name__)
    def log_warehouse_state(self, quota_dict: dict[int, dict[str, int]]) -> bool:
        if not quota_dict:
            return False

        try:
            sql = """
                INSERT INTO mp_data.a_wb_stock_transfer_warehouse_state_log (office_id, src_value, dst_value)
                VALUES (%s, %s, %s);
            """

            params: list[tuple[int, int, int]] = [
                (int(office_id), int(values.get('src', 0)), int(values.get('dst', 0)))
                for office_id, values in quota_dict.items()]

            self.db.execute_many(sql, params)

            return True

        except Exception as e:
            return False

    @simple_logger(logger_name=__name__)
    def insert_products_on_the_way(self,
                                   items: list[tuple[int, int, int, int, int]]) -> bool:

        if not items:
            return False

        sql = """INSERT INTO mp_data.a_wb_stock_transfer_products_on_the_way(nmId, 
                                                                            qty,
                                                                            qty_left_to_deliver,
                                                                            size_id,
                                                                            warehouse_from_id, 
                                                                            warehouse_to_id)
                VALUES (%s, %s, %s, %s, %s, %s)"""

        params = [(int(nm),int(qty),int(qty),  int(size_id), int(w_from), int(w_to)) for nm, qty, size_id, w_from, w_to in items]

        try:
            self.db.execute_many(sql, params)
            return True
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_all_products_with_stocks(self):

        sql = """WITH stock_with_max_time_end AS (
                                                SELECT wb_article_id, warehouse_id, size_id, MAX(time_end) AS max_time_end
                                                FROM mp_data.a_wb_catalog_stocks
                                                GROUP BY wb_article_id, warehouse_id, size_id)
                SELECT t.wb_article_id, t.warehouse_id, t.size_id, t.time_end, t.qty, awis.size
                FROM mp_data.a_wb_catalog_stocks t
                INNER JOIN stock_with_max_time_end m
                ON  t.wb_article_id = m.wb_article_id
                AND t.warehouse_id = m.warehouse_id
                AND t.size_id = m.size_id
                AND t.time_end = m.max_time_end
                LEFT JOIN mp_data.a_wb_izd_size awis ON awis.size_id = t.size_id;"""

        try:
            result = self.db.execute_query(sql)
            return result
        except Exception:
            return False
            
        
    @simple_logger(logger_name=__name__)
    def get_products_transfers_on_the_way(self):
        sql = """
            SELECT
                entry_id,
                nmId AS wb_article_id,
                warehouse_from_id,
                warehouse_to_id,
                size_id,
                qty_left_to_deliver as qty,
                created_at
            FROM mp_data.a_wb_stock_transfer_products_on_the_way
            WHERE created_at > NOW() - INTERVAL 14 day;"""
        
        try:
            return self.db.execute_query(sql)
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_products_transfers_on_the_way_dict(self):
        sql = """
            SELECT
                entry_id,
                nmId AS wb_article_id,
                warehouse_from_id,
                warehouse_to_id,
                o.region_id as region_to_id,
                size_id,
                qty,
                qty_left_to_deliver,
                created_at,
                finished_at,
                is_finished
            FROM mp_data.a_wb_stock_transfer_products_on_the_way p
            LEFT JOIN mp_data.a_wb_stock_transfer_wb_offices o 
            ON p.warehouse_to_id = o.office_id 
            WHERE created_at > NOW() - INTERVAL 14 day
            ORDER BY entry_id DESC;"""
        
        try:
            result = self.db.execute_query(sql)
            result_dict = defaultdict(list)

            for entry in result:
                date = entry['created_at'].strftime('%Y-%m-%d')
                index = f"{entry['wb_article_id']}_{entry['size_id']}_rto{entry['region_to_id']}_d{date}" # Индекс по SKU, размеру и складам
                result_dict[index].append(entry)

            return result_dict


        except Exception:
            return False
        

    @simple_logger(logger_name=__name__)
    def get_all_products_with_stocks_with_region(self):

        sql = """WITH stock_with_max_time_end AS (
                                                SELECT wb_article_id, warehouse_id, size_id, MAX(time_end) AS max_time_end
                                                FROM mp_data.a_wb_catalog_stocks
                                                GROUP BY wb_article_id, warehouse_id, size_id)
                SELECT t.wb_article_id, t.warehouse_id, t.size_id, t.time_end, t.qty, awis.size, awstww.region_id 
                FROM mp_data.a_wb_catalog_stocks t
                INNER JOIN stock_with_max_time_end m
                ON  t.wb_article_id = m.wb_article_id
                AND t.warehouse_id = m.warehouse_id
                AND t.size_id = m.size_id
                AND t.time_end = m.max_time_end
                LEFT JOIN mp_data.a_wb_izd_size awis ON awis.size_id = t.size_id
                LEFT JOIN mp_data.a_wb_stock_transfer_wb_warehourses awstww ON awstww.wb_office_id  = t.warehouse_id;"""

        try:
            result = self.db.execute_query(sql)
            return result
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_products_transfers_on_the_way_with_region(self):
        sql = """
            SELECT
                nmId AS wb_article_id,
                warehouse_from_id,
                warehouse_to_id,
                size_id,
                qty_left_to_deliver AS qty,
                awstww.region_id AS to_region_id,
                awstwf.region_id AS from_region_id,
                created_at
            FROM mp_data.a_wb_stock_transfer_products_on_the_way
            LEFT JOIN mp_data.a_wb_stock_transfer_wb_warehourses awstww
                ON awstww.wb_office_id = mp_data.a_wb_stock_transfer_products_on_the_way.warehouse_to_id
            LEFT JOIN mp_data.a_wb_stock_transfer_wb_warehourses awstwf
                ON awstwf.wb_office_id = mp_data.a_wb_stock_transfer_products_on_the_way.warehouse_from_id;"""
        
        try:
            return self.db.execute_query(sql)
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_current_regular_task(self):

        sql = """
            SELECT
                task_id,
                target_central,
                target_north_west,
                target_volga,
                target_south,
                target_urals,
                target_siberia,
                target_north_caucasus,
                target_far_east,
                min_central,
                min_north_west,
                min_volga,
                min_south,
                min_urals,
                min_siberia,
                min_north_caucasus,
                min_far_east,
                is_archived,
                task_creation_date,
                task_archiving_date,
                last_change_date
            FROM mp_data.a_wb_stock_transfer_regular_tasks
            WHERE is_archived = 0
            ORDER BY task_creation_date DESC
            LIMIT 1;"""
        try:
            result = self.db.execute_query(sql)
            if not result:
                return None
            return result[0]  
        except Exception:
            return None

    @simple_logger(logger_name=__name__)
    def get_warehouses_regions_map(self):
        sql = """
            SELECT wb_office_id, region_id
            FROM mp_data.a_wb_stock_transfer_wb_warehourses awstww
            WHERE wb_office_id IS NOT NULL
        """
        try:
            return self.db.execute_query(sql)
        except Exception:
            return False
        

    @simple_logger(logger_name=__name__)
    def get_real_warehouses_regions_map(self):
        sql = """
            SELECT warehouse_id, region_id
            FROM mp_data.a_wb_stock_transfer_wb_warehourses awstww
        """
        try:
            return self.db.execute_query(sql)
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_stocks_for_regular_tasks(self):
        sql = """WITH stocks AS (SELECT
                                    wb_article_id,
                                    warehouse_id,
                                    size_id,
                                    qty,
                                    time_end,
                    ROW_NUMBER() OVER (
                    PARTITION BY wb_article_id, warehouse_id, size_id
                    ORDER BY time_end DESC
                    ) AS rn
                FROM mp_data.a_wb_catalog_stocks
                )
                SELECT s.wb_article_id, s.warehouse_id, awis.size, s.size_id, s.time_end, s.qty, o.region_id
                FROM stocks s
                JOIN mp_data.a_wb_stock_transfer_wb_offices o
                ON s.warehouse_id = o.office_id
                LEFT JOIN mp_data.a_wb_izd_size awis
                ON awis.size_id = s.size_id
                WHERE o.region_id IS NOT NULL
                AND s.rn = 1
                AND s.time_end = (SELECT MAX(time_end) FROM mp_data.a_wb_catalog_stocks);"""
        try:
            return self.db.execute_query(sql)
        except Exception:
            return False

    @simple_logger(logger_name=__name__)
    def get_regions_with_sort_order(self):
        sql = """
            SELECT region_id, src_priority, dst_priority
            FROM mp_data.a_wb_stock_transfer_wb_regions regions
            WHERE src_priority is NOT NULL 
            AND dst_priority is NOT NULL;
        """
        try:
            result = self.db.execute_query(sql)
            result_dict = defaultdict(dict)
            for entry in result:
                result_dict[entry['region_id']] = {'src_priority':entry['src_priority'],
                                                    'dst_priority':entry['dst_priority']}

            return result_dict
        except Exception:
            return False
        


    @simple_logger(logger_name=__name__)   
    def get_warehouses_with_sort_order(self):
        sql = """
            SELECT office_id, src_priority, dst_priority, src_ignore, dst_ignore
            FROM mp_data.a_wb_stock_transfer_wb_offices offices
            WHERE src_priority is NOT NULL 
            AND dst_priority is NOT NULL;
        """
        try:
            result = self.db.execute_query(sql)
            result_dict = defaultdict(dict)
            for entry in result:
                result_dict[entry['office_id']] = {'src_priority':entry['src_priority'],
                                                    'dst_priority':entry['dst_priority']}
                if entry.get('src_ignore'):
                    result_dict[entry['office_id']]['src_priority'] = None
                if entry.get('dst_ignore'):
                    result_dict[entry['office_id']]['dst_priority'] = None

            return result_dict
        except Exception:
            return False
        

    @simple_logger(logger_name=__name__)
    def get_office_with_regions_map(self):
        sql = """
            SELECT office_id, region_id
            FROM mp_data.a_wb_stock_transfer_wb_offices
            WHERE src_priority is NOT NULL 
            AND dst_priority is NOT NULL; 
        """
        try:
            result = self.db.execute_query(sql)
            result_dict = defaultdict(list)
            for entry in result:
                result_dict[entry['region_id']].append(entry['office_id'])
            return result_dict
        except Exception:
            return False
        

    @simple_logger(logger_name=__name__)
    def get_stock_availability_data(self):
        sql = """
            SELECT wb_article_id, size_id, warehouse_id, time_beg, time_end
            FROM mp_data.a_wb_catalog_stocks;"""
        try:
            result = self.db.execute_query(sql)
            
            return result
        except Exception:
            return False
        
    @simple_logger(logger_name=__name__)
    def get_size_map(self):
        sql = """
            SELECT size, size_id
            FROM mp_data.a_wb_izd_size;"""
        try:
            result = self.db.execute_query(sql)
            result_dict = {}
            for entry in result:
                result_dict[entry['size']] = entry['size_id']
            return result_dict
            
        except Exception:
            return False
        

    @simple_logger(logger_name=__name__)
    def get_size_sales_for_warehouse(self):
        sql = """
            SELECT
                s.nmId,
                s.techSize_id,
                w.warehouse_wb_id AS office_id,
                COUNT(*) AS order_count
            FROM mp_data.a_wb_sales AS s
            LEFT JOIN mp_data.a_wb_warehouseName AS w
                ON s.warehouseName_id = w.warehouse_id
            WHERE s.last_update_time > NOW() - INTERVAL 60 DAY
                AND COALESCE(s.IsStorno, 0) = 0
                AND COALESCE(w.warehouse_wb_id, 0) <> 0
            GROUP BY s.nmId, s.techSize_id, w.warehouse_wb_id;"""
        try:
            result = self.db.execute_query(sql)
            
            return result
        except Exception:
            return False
        


    @simple_logger(logger_name=__name__)
    def get_blocked_warehouses_for_skus(self):
        sql = """SELECT * FROM mp_data.a_wb_stock_transfer_products_on_the_way 
                WHERE created_at > NOW() - INTERVAL 1 day;"""
        try:
            result = self.db.execute_query(sql)
            result_dict = defaultdict(list)
            for entry in result:
                index = f"{entry['nmId']}_{entry['size_id']}" # Индекс по SKU и размеру
                result_dict[index].append(entry['warehouse_from_id'])
            
            return result_dict
        except Exception:
            return False
        


    @simple_logger(logger_name=__name__)
    def update_product_transfer_entries(self, entries):

        if not entries:
            return False

        try:
            sql = """UPDATE mp_data.a_wb_stock_transfer_products_on_the_way
                        SET qty_left_to_deliver = %s,
                            is_finished = %s,
                            finished_at = %s
                        WHERE entry_id = %s;"""

            params: list[tuple[int, int, str, int]] = []
            for entry in entries:
                params.append((int(entry['qty_left_to_deliver']),
                               int(entry.get('is_finished', 0)),
                               entry.get('finished_at', None),
                               int(entry['entry_id'])))

            self.db.execute_many(sql, params)

            return True

        except Exception as e:
            
            return False
        

    @simple_logger(logger_name=__name__)
    def get_wb_supply_destinations(self):
        sql = """SELECT * FROM mp_data.a_wb_stock_transfer_wb_supply_destination_cl"""
        try:
            result = self.db.execute_query(sql)
            result_dict = {}
            for entry in result:
                result_dict[entry['destination_name']] = entry['region_id']
                
            return result_dict
        except Exception:
            return False

