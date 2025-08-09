from collections import defaultdict
import time
from requests.cookies import RequestsCookieJar
from typing import Dict, Union, List, Optional, Tuple
import logging

# Модели
from models.tasks import ProductToTask, TaskWithProducts, ProductSizeInfo

# Зависимости
from infrastructure.api.sync_controller import SyncAPIController
from infrastructure.db.mysql.mysql_controller import MySQLController


class OneTimeTaskProcessor:
    def __init__(self,
                 api_controller: SyncAPIController,
                 db_controller: MySQLController,
                 cookie_jar: RequestsCookieJar,
                 headers: Dict,
                 logger):
        self.api_controller = api_controller
        self.db_controller = db_controller
        self.cookie_jar = cookie_jar
        self.headers = headers
        self.logger = logger


    def process_one_time_tasks(self):
        self.logger.info("Старт process_one_time_tasks()")
        try:

            tasks = self.db_controller.get_all_tasks_with_products_dict()
            self.logger.debug("Получено заданий: %s", len(tasks) if tasks else 0)
            random_nmid = self.db_controller.get_max_stock_article()  # Прост рандомный артикул со стоками
            self.logger.debug("Получен случайный nmid для стоков: %s", random_nmid)
            office_id_list = self.fetch_warehouse_list(random_present_nmid=random_nmid)
            self.logger.debug("Склады (office_id_list): %s", office_id_list)

            if not office_id_list:
                self.logger.warning("Список складов пуст. Останавливаю обработку.")
                return

            # Получить все квоты
            quota_dict = dict(self.get_warehouse_quotas(office_id_list))

            # quota_dict = {507: {'src': 39835, 'dst': 0}, 117986: {'src': 41436, 'dst': 0}, 120762: {'src': 0, 'dst': 74184}, 2737: {'src': 24709, 'dst': 0}, 130744: {'src': 0, 'dst': 459945}, 686: {'src': 24992, 'dst': 0}, 1733: {'src': 24340, 'dst': 0}, 206348: {'src': 10000, 'dst': 72554}, 208277: {'src': 0, 'dst': 84267}, 301760: {'src': 0, 'dst': 93503}, 301809: {'src': 0, 'dst': 493275}, 301983: {'src': 0, 'dst': 3355}}

            self.logger.info("Квоты складов получены: %s записей", len(quota_dict))
        
            # Логаем текущие квоты
            self.db_controller.log_warehouse_state(quota_dict)
            self.logger.debug("Состояние складов залогировано в БД")

        except Exception as e:
            self.logger.exception("Ошибка обработке разовых заданий: %s", e)

        # Пошли обрабатывать задания
        for task_idx, task in enumerate(tasks.values(), start=1):
            self.logger.info("Обработка задания #%s", task_idx)

            try:
                # Проверяем, есть ли для задания квоты на перемещение
                available_warehouses_from_ids, available_warehouses_to_ids = self.get_available_warehouses_by_quota(quota_dict=quota_dict)
                self.logger.debug("Доступные склады-источники: %s; склады-получатели: %s",
                                  available_warehouses_from_ids, available_warehouses_to_ids)
                if not available_warehouses_from_ids or not available_warehouses_to_ids:
                    self.logger.warning("Нет доступных складов по квотам. Пропускаю задание.")
                    continue

            except Exception as e:
                self.logger.exception("Ошибка при определении доступных складов: %s", e)
                continue

            # По каждому продукту в задании проводим итерацию
            for product_idx, product in enumerate(getattr(task, "products", []) , start=1):
                self.logger.info("Задание #%s: обработка продукта #%s (nmID=%s)", task_idx, product_idx, getattr(product, "product_wb_id", None))
                
                try:
                    # Cмотрим остатки по всем складам
                    product_stocks = self.fetch_stocks_by_nmid(
                        nmid=product.product_wb_id,
                        warehouses_in_task_list=available_warehouses_from_ids)
                    
                    self.logger.debug("Стоки для nmID=%s: %s", getattr(product, "product_wb_id", None), product_stocks)

                except Exception as e:
                    self.logger.exception("Ошибка при получении стоков для продукта nmID=%s: %s", getattr(product, "product_wb_id", None), e)
                    product_stocks = None

                # Если нет остатков
                if not product_stocks:
                    self.logger.info("Остатков нет для nmID=%s. Пропуск.", getattr(product, "product_wb_id", None))
                    continue

                # Для каждого склада донора из доступных
                for src_warehouse_id in available_warehouses_from_ids:
                    warehouse_quota_src = quota_dict[src_warehouse_id]['src']  # Если квота на нуле, пропускаем
                    if warehouse_quota_src < 1:
                        self.logger.debug(f"Недостаточно квоты на складе-доноре. src: {src_warehouse_id} - {src_warehouse_quota}")
                        continue

                    self.logger.debug("Обработка склада-донора src_warehouse_id=%s", src_warehouse_id)
                    current_warehouse_transfer_request_bodies = defaultdict(dict)  # Записи трансфера по донору на каждое наставление

                    for size in getattr(product, "sizes", []):
                        try:
                            if size.transfer_qty_left_virtual <= 0:
                                self.logger.debug("Размер %s: transfer_qty_left_virtual<=0, пропуск", getattr(size, "size_id", None))
                                continue

                            self.logger.debug("Создание позиций для size_id=%s (осталось виртуально=%s)",
                                              getattr(size, "size_id", None), getattr(size, "transfer_qty_left_virtual", None))

                            # Заполняем записи под размер
                            self.create_single_size_entries(
                                src_warehouse_id=src_warehouse_id,
                                size=size,
                                product_stocks=product_stocks,
                                available_warehouses_to_ids=available_warehouses_to_ids,
                                quota_dict=quota_dict,
                                current_warehouse_transfer_request_bodies=current_warehouse_transfer_request_bodies)
                            
                        except Exception as e:
                            self.logger.exception("Ошибка при создании записей для size_id=%s: %s", getattr(size, "size_id", None), e)

                    for dst_warehouse_id, warehouse_entries in current_warehouse_transfer_request_bodies.items():

                        dst_warehouse_quota = quota_dict[dst_warehouse_id]['dst'] # Если квота на нуле, пропускаем
                        src_warehouse_quota = quota_dict[src_warehouse_id]['src']

                        if dst_warehouse_quota < 1 or src_warehouse_quota < 1:
                            self.logger.debug(f"На одном из складов. src: {src_warehouse_id} - {src_warehouse_quota} | dst: {dst_warehouse_id} - {dst_warehouse_quota}")

                            continue

                        try:
                            self.logger.debug("Формирование тела заявки: src=%s -> dst=%s; entries=%s",
                                              src_warehouse_id, dst_warehouse_id, warehouse_entries)

                            warehouse_req_body = self.create_transfer_request_body(
                                src_warehouse_id=src_warehouse_id,
                                dst_wrh_id=dst_warehouse_id,
                                product=product,
                                warehouse_entries=warehouse_entries)

                            self.logger.info("Готово тело заявки для отправки: %s", warehouse_req_body)
                            try:
                                print(f"POST: {warehouse_req_body}")
                                self.logger.debug("Отправка заявки: %s", warehouse_req_body)

                                response = self.send_transfer_request(warehouse_req_body)
                                if response.status_code in [200, 201, 202, 204]:
                                # mock_true = True
                                # if mock_true:
                                    for size in getattr(product, "sizes", []):
                                        if size.size_id in warehouse_entries:
                                            size.transfer_qty_left_real -= warehouse_entries[size.size_id]['count']
                                            self.logger.debug(
                                                "Обновлен transfer_qty_left_real для size_id=%s: -%s",
                                                size.size_id, warehouse_entries[size.size_id]['count'])
                                            quota_dict[src_warehouse_id]['src'] -= warehouse_entries[size.size_id]['count']
                                            quota_dict[dst_warehouse_id]['dst'] -= warehouse_entries[size.size_id]['count']
                                            
                            except Exception as e:
                                print(f"Ошибка при отправке запроса: {e}")
                                self.logger.exception("Ошибка при отправке запроса: %s", e)

                        except Exception as e:
                            self.logger.exception("Ошибка при подготовке/отправке заявки src=%s dst=%s: %s",
                                                  src_warehouse_id, dst_warehouse_id, e)

            try:
                self.db_controller.update_transfer_qty_from_task(task)  # Тут в БД несем задания
                self.logger.info("Задание #%s: обновлены количества трансферов в БД", task_idx)
            except Exception as e:
                self.logger.exception("Ошибка при обновлении задания #%s в БД: %s", task_idx, e)

        self.logger.info("Завершение process_one_time_tasks()")


    @staticmethod
    def find_product_with_max_transfer_qty(tasks_dict: dict[int, TaskWithProducts]) -> Union[int, None]:
        logging.getLogger(__name__).debug("Старт find_product_with_max_transfer_qty()")
        max_product_wb_id = None
        max_qty = -1

        try:
            for task in tasks_dict.values():
                for product in task.products:
                    for size in product.sizes:
                        if size.transfer_qty > max_qty:
                            max_qty = size.transfer_qty
                            max_product_wb_id = product.product_wb_id
            logging.getLogger(__name__).debug("Максимальный nmID=%s при qty=%s", max_product_wb_id, max_qty)
        except Exception as e:
            logging.getLogger(__name__).exception("Ошибка в find_product_with_max_transfer_qty: %s", e)

        return max_product_wb_id

    def fetch_warehouse_list(self, random_present_nmid: int):
        self.logger.debug("Запрос списка складов по nmID=%s", random_present_nmid)
        try:
            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="GET",
                endpoint="/ns/shifts/analytics-back/api/v1/stocks",
                params={"nmID": str(random_present_nmid)},
                cookies=self.cookie_jar,
                headers=self.headers)
            
            self.logger.debug("Ответ получен: status=%s", getattr(response, "status_code", None))

            response_json = response.json()
            dst_data = response_json.get("data", {}).get("dst", [])
            office_id_list = [w.get("officeID") for w in dst_data if "officeID" in w]
            self.logger.info("Получено офисов (dst): %s", len(office_id_list))
            return office_id_list

        except Exception as e:
            self.logger.exception("Ошибка в fetch_warehouse_list: %s", e)
            return None

    def get_warehouse_quotas(self, office_id_list):
        self.logger.debug("Старт get_warehouse_quotas() для офисов: %s", office_id_list)
        quota_dict = defaultdict(dict)
        modes = ["src", "dst"]

        for office_id in office_id_list:
            for mode in modes:
                try:
                    time.sleep(0.5)
                    self.api_controller.request(
                        base_url="https://seller-weekly-report.wildberries.ru",
                        method="OPTIONS",
                        endpoint="/ns/shifts/analytics-back/api/v1/quota",
                        params={"officeID": office_id, "type": mode},
                        cookies=self.cookie_jar,
                        headers=self.headers)
                        
                    self.logger.debug("OPTIONS квоты отправлен office_id=%s mode=%s", office_id, mode)

                    time.sleep(0.5)
                    response = self.api_controller.request(
                        base_url="https://seller-weekly-report.wildberries.ru",
                        method="GET",
                        endpoint="/ns/shifts/analytics-back/api/v1/quota",
                        params={"officeID": office_id, "type": mode},
                        cookies=self.cookie_jar,
                        headers=self.headers)
                    
                    self.logger.debug("GET квоты получен office_id=%s mode=%s status=%s",
                                      office_id, mode, getattr(response, "status_code", None))

                    response_json = response.json()
                    response_data = response_json.get("data", {})
                    response_quota = response_data.get("quota", 0)
                    quota_dict[office_id][mode] = response_quota
                    self.logger.debug("Квота office_id=%s mode=%s = %s", office_id, mode, response_quota)
                except Exception as e:
                    self.logger.exception("Ошибка получения квоты office_id=%s mode=%s: %s", office_id, mode, e)

        self.logger.info("Итоговые квоты получены: %s офисов", len(quota_dict))
        return quota_dict

    def get_available_warehouses_by_quota(self, quota_dict: Dict[int, Dict[str, int]]) -> Tuple[List[int], List[int]]:
        self.logger.debug("Расчёт доступных складов по квотам")
        try:
            available_warehouses_from_ids = [wid for wid, q in quota_dict.items() if q.get('src', 0) != 0]
            available_warehouses_to_ids = [wid for wid, q in quota_dict.items() if q.get('dst', 0) != 0]
            self.logger.info("Доступно from: %s; to: %s",
                             len(available_warehouses_from_ids), len(available_warehouses_to_ids))
            return available_warehouses_from_ids, available_warehouses_to_ids
        except Exception as e:
            self.logger.exception("Ошибка в get_available_warehouses_by_quota: %s", e)
            return [], []

    def create_single_size_entries(self,
                                   src_warehouse_id: int,
                                   size: ProductSizeInfo,
                                   product_stocks: dict,
                                   available_warehouses_to_ids: list,
                                   quota_dict: dict,
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

                for dst_warehouse_id in available_warehouses_to_ids:
                    if size.transfer_qty_left_virtual > 0 and move_qty > 0:
                        try:
                            dst_quota = quota_dict[dst_warehouse_id]['dst']
                        except Exception as e:
                            self.logger.exception("Ошибка доступа к квоте dst для склада %s: %s", dst_warehouse_id, e)
                            dst_quota = 0

                        if dst_quota > 0:
                            transfer_amount = min(dst_quota, move_qty, available_qty)
                            request_count_entry = {
                                "chrtID": stock_entry["chrtID"],
                                "count": transfer_amount}
                            
                            current_warehouse_transfer_request_bodies[dst_warehouse_id][size_id] = request_count_entry
                            self.logger.debug("Добавлена запись в запрос: dst=%s size_id=%s amount=%s",
                                              dst_warehouse_id, size_id, transfer_amount)
                            available_qty -= transfer_amount
                            size.transfer_qty_left_virtual -= transfer_amount
                            move_qty -= 1
                            
        except Exception as e:
            self.logger.exception("Ошибка в create_single_size_entries: %s", e)

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

    def fetch_stocks_by_nmid(self, nmid: int, warehouses_in_task_list: list):
        self.logger.debug("Запрос стоков по nmID=%s для складов: %s", nmid, warehouses_in_task_list)
        try:
            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="GET",
                endpoint="/ns/shifts/analytics-back/api/v1/stocks",
                params={"nmID": str(nmid)},
                cookies=self.cookie_jar,
                headers=self.headers)
            
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
            return stock_by_warehouse_dict

        except Exception as e:
            self.logger.exception("Ошибка в fetch_stocks_by_nmid nmID=%s: %s", nmid, e)
            return None

    def send_transfer_request(self, request_body: dict):
        self.logger.debug("Отправка transfer request: %s", request_body)
        try:
            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="POST",
                endpoint="/ns/shifts/analytics-back/api/v1/order",
                json=request_body,
                cookies=self.cookie_jar,
                headers=self.headers)
            
            self.logger.info("Ответ на transfer request: status=%s", getattr(response, "status_code", None))
            return response

        except Exception as e:
            self.logger.exception("Ошибка в send_transfer_request: %s", e)
            return None
