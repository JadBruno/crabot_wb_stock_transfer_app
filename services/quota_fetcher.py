import asyncio
import aiohttp
import time


class QuotaFetcher:
    def __init__(self, api_controller,
                 mysql_controller,
                 cookie_list,
                 headers,
                 logger,
                 cooldown: float = 0.5):
        self.api_controller = api_controller
        self.mysql_controller = mysql_controller
        self.cookie_list = cookie_list
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

        session = aiohttp.ClientSession()

        results = []

        start_time = time.perf_counter()

        self.logger.info(f"Начинаем получение лимитов по складам")

        for office_id in office_id_list:
            for mode in modes:
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
                await asyncio.sleep(self.cooldown/self.cooldown)

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

        await session.close()

        quota_dict = {}
        for office_id, mode, quota in results:
            if office_id not in quota_dict:
                quota_dict[office_id] = {}
            quota_dict[office_id][mode] = quota

        self.quota_dict = quota_dict
        return quota_dict
    

    async def _fetch_single_quota(self, office_id, mode, cookies, tokenv3, session:aiohttp.ClientSession):

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
                    return office_id, mode, None
                
            await asyncio.sleep(0.1)  # Небольшая пауза между запросами

            async with session.get(
                "https://seller-weekly-report.wildberries.ru/ns/shifts/analytics-back/api/v1/quota",
                params={"officeID": office_id, "type": mode}) as resp:
                response_json = await resp.json()
                quota = response_json.get("data", {}).get("quota", 0)

            return office_id, mode, quota
