from collections import defaultdict
from infrastructure.db.mysql.base import SyncDatabase
from models.tasks import TaskWithProducts, ProductToTask, ProductSizeInfo
import json

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

