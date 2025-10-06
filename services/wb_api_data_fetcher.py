import asyncio
import aiohttp
import time
from infrastructure.api.sync_controller import SyncAPIController
from infrastructure.db.mysql.mysql_controller import MySQLController

class WBAPIDataFetcher:
    def __init__(self, api_controller: SyncAPIController,
                 mysql_controller: MySQLController,
                 cookie_list,
                 wb_content_api_key,
                 headers,
                 logger,
                 cooldown: float = 0.5):
        self.api_controller = api_controller
        self.mysql_controller = mysql_controller
        self.cookie_list = cookie_list
        self.wb_content_api_key = wb_content_api_key
        self.headers = headers
        self.logger = logger
        self.quota_dict = None
        self.cooldown = cooldown
        
        # Локи и время последнего запроса на каждый cookie
        self._cookie_locks = [asyncio.Lock() for _ in self.cookie_list]
        self._cookie_last_time = [0.0 for _ in self.cookie_list]
        self.session = None


    async def fetch_quota(self, office_id_list):
        if self.quota_dict is not None:
            return self.quota_dict

        tasks = []
        modes = ['dst', 'src']

        cookie_index = 0
        cookie_count = len(self.cookie_list)

        # self.cookie_list.pop(-1)

        async with aiohttp.ClientSession() as session:

            results = []

            start_time = time.perf_counter()

            self.logger.info(f"Начинаем получение лимитов по складам")

            for office_id in office_id_list:
                for mode in modes:

                    cookie_count = len(self.cookie_list)

                    for _ in range(cookie_count):
                        if cookie_count == 0:
                            self.logger.error("Список cookies пуст. Невозможно выполнить запрос квоты.")
                            return 0
                        
                        if cookie_index >= cookie_count:
                            cookie_index = 0

                            

                    cur_cookie_data = self.cookie_list[cookie_index]
                    cur_cookies = cur_cookie_data['cookies']
                    cur_tokenv3 = cur_cookie_data['tokenV3']
                    tasks.append(
                        self._fetch_single_quota(
                            office_id, mode, cookies=cur_cookies, tokenv3=cur_tokenv3, session=session))
                    cookie_index = (cookie_index + 1) % cookie_count
                    
                    current_batch_result = await asyncio.gather(*tasks)
                    results.extend(current_batch_result)
                    tasks = []

                    if cookie_count > 0:
                        await asyncio.sleep(self.cooldown/cookie_count)

                    # if len(tasks) == cookie_count:
                    #     current_batch_result = await asyncio.gather(*tasks)
                    #     results.extend(current_batch_result)
                    #     tasks = []
                    #     await asyncio.sleep(self.cooldown)
            
            if tasks:
                last_batch_results = await asyncio.gather(*tasks)
                results.extend(last_batch_results)

            # results = await asyncio.gather(*tasks)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            self.logger.info(f"Лимиты получены за: {elapsed_time:.2f} секунд")

        quota_dict = {}
        for office_id, mode, quota in results:
            if office_id not in quota_dict:
                quota_dict[office_id] = {}
            quota_dict[office_id][mode] = quota

        self.quota_dict = quota_dict
        return quota_dict
    

    async def _fetch_single_quota(self, office_id, mode, cookies, tokenv3, session:aiohttp.ClientSession):
            
        try:

            headers = self.headers.copy()
            headers['AuthorizeV3'] = tokenv3

            session.headers.update(headers)
            session.cookie_jar.update_cookies(cookies)

            async with session.options(
                "https://seller-weekly-report.wildberries.ru/ns/shifts/analytics-back/api/v1/quota",
                params={"officeID": office_id, "type": mode}) as resp:
                if resp.status not in (200, 201, 202, 203, 204):
                    self.logger.error(
                        "Ответ OPTIONS от ВБ не соответствует ожиданию, office_id=%s mode=%s",
                        office_id, mode)
                    return office_id, mode, -1
                
            await asyncio.sleep(0.1)  # Небольшая пауза между запросами

            async with session.get(
                "https://seller-weekly-report.wildberries.ru/ns/shifts/analytics-back/api/v1/quota",
                params={"officeID": office_id, "type": mode}) as resp:
                if resp.status not in (200, 201, 202, 203, 204):
                    self.logger.error(
                        "Ответ GET от ВБ не соответствует ожиданию, office_id=%s mode=%s",
                        office_id, mode)
                    return office_id, mode, -1
                
                response_json = await resp.json()
                quota = response_json.get("data", {}).get("quota", 0)

            return office_id, mode, quota
        
        except:
            self.logger.error(
                        "Неизвестная ошибка для, office_id=%s mode=%s",
                        office_id, mode)
            return office_id, mode, quota


    def fetch_warehouse_list(self, random_present_nmid) -> list[int] | None:
        try:
            cookie_count = len(self.cookie_list)

            for _ in range(cookie_count):

                if cookie_count == 0:
                    self.logger.error("Список cookies пуст. Невозможно выполнить запрос списка складов.")
                    return None
                if self.current_cookie_index >= cookie_count:
                    self.current_cookie_index = 0

                cookies = self.cookie_list[self.current_cookie_index]['cookies']
                tokenV3 = self.cookie_list[self.current_cookie_index]['tokenV3']

                headers = self.headers.copy()
                headers['AuthorizeV3'] = tokenV3


                response = self.api_controller.request(
                    base_url="https://seller-weekly-report.wildberries.ru",
                    method="GET",
                    endpoint="/ns/shifts/analytics-back/api/v1/stocks",
                    params={"nmID": str(random_present_nmid)},
                    cookies=cookies,
                    headers=headers)
                
                self.logger.debug("Ответ получен: status=%s", getattr(response, "status_code", None))

                if response.status_code in (401, 403):
                    self.logger.error("Ответ от ВБ не соответствует ожиданию, status=%s",
                                      getattr(response, "status_code", None))
                    self.cookie_list.pop(self.current_cookie_index)
                    self.current_cookie_index = (self.current_cookie_index + 1) % len(self.cookie_list)
                    continue

                response_json = response.json()
                dst_data = response_json.get("data", {}).get("dst", [])
                office_id_list = [w.get("officeID") for w in dst_data if "officeID" in w]
                self.logger.info("Получено офисов (dst): %s", len(office_id_list))
                self.current_cookie_index = (self.current_cookie_index + 1) % len(self.cookie_list)
                return office_id_list

        except Exception as e:
            self.logger.exception("Ошибка в fetch_warehouse_list: %s", e)
            return None
        


    def fetch_all_chrtID(self) -> dict | None:
        
        chrtid_by_techsize_dict = {}

        req_attempts = 0

        api_key = self.wb_content_api_key['API key']

        headers = {'Authorization': api_key,
                        'Content-Type': 'application/json'}

        response_card_limit = 100
        
        request_cursor = {'limit':response_card_limit}

        try:

            while req_attempts <= 1000:

            
                    req_body = {"settings": {"filter": {"withPhoto": -1},
                                            "cursor": request_cursor}}


                    response = self.api_controller.request(
                        base_url="https://content-api.wildberries.ru",
                        method="POST",
                        endpoint="/content/v2/get/cards/list",
                        json=req_body,
                        headers=headers)
                    
                    self.logger.debug("Ответ получен: status=%s", getattr(response, "status_code", None))

                    response_json = response.json()
                    cards = response_json.get("cards", [])
                    for card in cards:
                        sizes = card.get("sizes", [])
                        for size in sizes:
                            tech_size = size.get("techSize", None)
                            chrt_id = size.get("chrtID", None)
                            if tech_size and chrt_id and tech_size not in chrtid_by_techsize_dict:
                                chrtid_by_techsize_dict[tech_size] = chrt_id
                    
                    response_cursor = response_json.get("cursor", {})
                    response_total = response_cursor.get("total", 0)
                    last_item_id = response_cursor.get("nmID", None)

                    if response_total < response_card_limit or not last_item_id or response_total == 0:
                        self.logger.info("Все chrtID получены. Всего: %s", len(chrtid_by_techsize_dict))
                        break

                    request_cursor['limit'] = response_total
                    request_cursor['nmID'] = last_item_id

                    req_attempts += 1

            return chrtid_by_techsize_dict

        except Exception as e:
            self.logger.exception("Ошибка в fetch_warehouse_list: %s", e)
            return None
        



    def fetch_chrtids_for_nmId_list(self, nmId_list) -> dict | None:
        
        chrtid_entries = []

        req_attempts = 0

        api_key = self.wb_content_api_key['API key']

        headers = {'Authorization': api_key,
                        'Content-Type': 'application/json'}

        response_card_limit = 1
        
        request_cursor = {'limit':response_card_limit}

        for nm_id in nmId_list:

            try:

                
                req_body = {"settings": {"filter": {"withPhoto": -1,
                                                    "nmID": nm_id},
                                        "cursor": request_cursor}}


                response = self.api_controller.request(
                    base_url="https://content-api.wildberries.ru",
                    method="POST",
                    endpoint="/content/v2/get/cards/list",
                    json=req_body,
                    headers=headers)
                
                self.logger.debug("Ответ получен: status=%s", getattr(response, "status_code", None))

                response_json = response.json()
                cards = response_json.get("cards", [])
                for card in cards:
                    sizes = card.get("sizes", [])
                    for size in sizes:
                        tech_size = size.get("techSize", None)
                        chrt_id = size.get("chrtID", None)
                        nmID = card.get("nmID", None)
                        if tech_size and chrt_id and nmID:
                            chrt_ir_entry = {'nmID': nmID,
                                            'techSize': tech_size,
                                            'chrtID': chrt_id}
                            chrtid_entries.append(chrt_ir_entry)

            except Exception as e:
                self.logger.exception("Ошибка в fetch_warehouse_list: %s", e)
                return None

        return chrtid_entries


    @staticmethod
    def check_cookie_list(self, cookie_list_original, random_present_nmid):

        self.logger.info('Проверяем кукис на работоспособность')

        cookie_list_filtered = []

        all_office_id_list = []

        for cookie_data in cookie_list_original:

            cookies = cookie_data['cookies']
            tokenV3 = cookie_data['tokenV3']

            headers = self.headers.copy()
            headers['AuthorizeV3'] = tokenV3


            response = self.api_controller.request(
                base_url="https://seller-weekly-report.wildberries.ru",
                method="GET",
                endpoint="/ns/shifts/analytics-back/api/v1/stocks",
                params={"nmID": str(random_present_nmid)},
                cookies=cookies,
                headers=headers)
            
            self.logger.debug("Ответ получен: status=%s", getattr(response, "status_code", None))

            time.sleep(1)

            if response.status_code in (200, 201):

                cookie_list_filtered.append(cookie_data)

                if all_office_id_list == []:

                    response_json = response.json()
                    dst_data = response_json.get("data", {}).get("dst", [])
                    all_office_id_list = [w.get("officeID") for w in dst_data if "officeID" in w]
                    self.logger.info("Получено офисов (dst): %s", len(all_office_id_list))

            else:
                self.logger.error('Полученный ответ не соответсвует ожиданиям')

        self.cookie_list = cookie_list_filtered
        result = {'cookies':cookie_list_filtered, 'office_id_list':all_office_id_list}

        return result   

                
                