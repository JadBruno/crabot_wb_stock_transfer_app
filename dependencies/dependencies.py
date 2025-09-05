from utils.config import cookies_decrypt_key, default_headers, cookie_access_name_array
from utils.cookies_parser import CookieDecryptor
from infrastructure.api.sync_controller import SyncAPIController
from utils.logger import get_logger
from utils.access_data_loader import AccessDataLoader
from infrastructure.db.mysql.base import SyncDatabase
from infrastructure.db.mysql.mysql_controller import MySQLController
from functools import cached_property

class Dependencies:
    def __init__(self):
        self._logger = get_logger("stock_transfer")
        self.cookie_utils = CookieDecryptor(key_base64=cookies_decrypt_key)
        self.api_controller = SyncAPIController()
        self.access_data_loader = AccessDataLoader(logger=self.logger)
        self._mysql_controller = None
        self._cookie_jar = None
        self._authorized_headers = None
        self._wb_analytics_api_key = None


    @property
    def logger(self):
        if self._logger is None:
            self._logger = get_logger("stock_transfer")
        return self._logger

    @property
    def mysql_controller(self):
        if self._mysql_controller is None:
            mysql_connect_params_dict = self.access_data_loader.get_mysql_connect_params_dict()
            con_data = mysql_connect_params_dict['no_db_fixed']
            db = SyncDatabase(host=con_data['host'],
                                port=con_data['port'],
                                user=con_data['user'],
                                password=con_data['password'],
                                db='dostup')
            self._mysql_controller = MySQLController(db=db)
        return self._mysql_controller

    @cached_property
    def cookie_list(self):
        cookie_list = self.fetch_all_cookies()
        return cookie_list
    
    @cached_property
    def cookie_jar(self):
        if self._cookie_jar is None:
            self._cookie_jar = self.cookie_list[0]['cookies']
        return self._cookie_jar
        


    @cached_property
    def authorized_headers(self):
        if self._authorized_headers is not None:
            return self._authorized_headers
        else:
            headers_copy = default_headers.copy()
            tokenV3 = self.cookie_list[0]['tokenV3']
            headers_copy['AuthorizeV3'] = tokenV3
            return headers_copy
    
    @cached_property
    def wb_analytics_api_key(self):
        if self._wb_analytics_api_key is None:
            self._wb_analytics_api_key = self.access_data_loader.get_wb_analytics_api_key()
        return self._wb_analytics_api_key
    

    def fetch_all_cookies(self):
        cookie_list = []
        for name in cookie_access_name_array:
            try:
                cookies_orig_data, tokenV3 = self.access_data_loader.fill_cookie_access_data(access_name=name)
                cookie_parsed_data = self.cookie_utils.parse_cookie_string(cookies_orig_data)
                if cookie_list == []:
                    self._cookie_jar = cookie_parsed_data
                    headers_copy = default_headers.copy()
                    headers_copy['AuthorizeV3'] = tokenV3
                    self._authorized_headers = headers_copy

                cookies_data = {'cookies': cookie_parsed_data, 'tokenV3': tokenV3}
                cookie_list.append(cookies_data)
                
            except Exception as e:
                self.logger.exception(f"Ошибка при получение cookies по имени {name}: {e}")
        return cookie_list


deps = Dependencies()

logger = deps.logger
cookie_jar = deps.cookie_jar
authorized_headers = deps.authorized_headers
mysql_controller = deps.mysql_controller
api_controller = SyncAPIController()
wb_analytics_api_key = deps.wb_analytics_api_key
cookie_list = deps.cookie_list