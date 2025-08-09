from utils.secer_module_v1_1e import SecurityModule

class AccessDataLoader:
        def __init__(self,logger):
                self.__sec_mod = SecurityModule()

                self.__logger = logger
                self.__mysql_connect_params_dict = self.fill_mysql_access_data()
                self.__cookies = self.fill_cookie_access_data()
                self.__tokenV3 = self.fill_tokenv3_access_data()

        
        def simple_logger(func):

                def wrapper(self, *args, **kwargs):
                        self.__logger.debug(f'Запускаем функцию {func.__name__}()')

                        try:
                                result = func(self, *args, **kwargs)
                                return result
                        
                        except Exception as e:
                                self.__logger.exception(f"Ошибка в {func.__name__}():\n {e}")

                        return wrapper


        def create_mysql_connect_params_no_db_fixed(self, mysql_connect_param_mp_data):

                mysql_connect_params_no_db_fixed = mysql_connect_param_mp_data.copy()
                del mysql_connect_params_no_db_fixed['db']
                return mysql_connect_params_no_db_fixed


        def fill_mysql_access_data(self):
                """ Заполняет словарь доступов к БД """

                mysql_connect_params_dict = {}

                try:
                        mysql_connect_param_mp_data = self.__sec_mod.get_access_data('MySQL параметры подключения к БД mp_data')['MySQL параметры подключения к БД mp_data']
                        self.__logger.debug("Получение параметров подключения к БД mp_data")
                        mysql_connect_param_mp_data['port'] = int(mysql_connect_param_mp_data['port'])
                        mysql_connect_params_no_db_fixed = self.create_mysql_connect_params_no_db_fixed(mysql_connect_param_mp_data)
                        self.__logger.debug("Порты подключений в БД преобразованы из str в int")
                        mysql_connect_params_dict = {'no_db_fixed':mysql_connect_params_no_db_fixed,
                                                        'mp_data': mysql_connect_param_mp_data}

                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return mysql_connect_params_dict
        
        def fill_cookie_access_data(self):
                """ Заполняет словарь доступов к БД """

                try:
                        cookie_data = self.__sec_mod.get_access_data('Cookies')['Cookies']
                        self.__logger.debug("Получение cookies")


                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return cookie_data
        
        def fill_tokenv3_access_data(self):
                """ Заполняет словарь доступов к БД """

                try:
                        tokenV3 = self.__sec_mod.get_access_data('WBTokenV3')['WBTokenV3']
                        self.__logger.debug("Получение tokenV3")

                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return tokenV3
        

        def fill_api_access_data(self):

                ozon_api_access_dict = {}
        
                try:
                
                        seller_api_data = self.__sec_mod.get_access_data("Ozon Seller Admin API")["Ozon Seller Admin API"]
                        self.__logger.debug("Получение токена доступа к Ozon Seller API")
                        ozon_api_access_dict['ozon_client_id'] = seller_api_data['Client ID']
                        self.__logger.debug("В ozon_api_access_dict добавлен ozon_client_id")
                        ozon_api_access_dict['ozon_api_key'] = seller_api_data['API key']
                        self.__logger.debug("В ozon_api_access_dict добавлен ozon_api_key")
                        self.__logger.debug("Успешное заполнение ozon_api_access_dict")
                
                except Exception as e:
                
                        self.__logger.exception(f"Ошибка при получение доступов Ozon API: {e}")
                
                return ozon_api_access_dict
        


        def get_mysql_connect_params_dict(self):

                return self.__mysql_connect_params_dict
        
        
        def get_cookies(self):
                return self.__cookies
        
        def get_tokenV3(self):
                return self.__cookies
        
