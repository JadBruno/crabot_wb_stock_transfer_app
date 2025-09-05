from utils.secer_module_v1_1e import SecurityModule

class AccessDataLoader:
        def __init__(self,logger):
                self.__sec_mod = SecurityModule()

                self.__logger = logger
                self.__mysql_connect_params_dict = self.fill_mysql_access_data()
                # self.__cookies = self.fill_cookie_access_data()
                # self.__tokenV3 = self.fill_tokenv3_access_data()
                self.__wb_analytics_api_key = self.fill_api_key_access_data()


        
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
        
        def fill_cookie_access_data(self, access_name='Wildberries Seller ЛК Cookies с доступом Поставки'):
                """ Заполняет словарь доступов к БД """
                cookie_data = {}
                try:
                        # cookie_data = self.__sec_mod.get_access_data('Wildberries Seller ЛК Cookies с доступом Поставки')['Cookies']
                        data = self.__sec_mod.get_access_data(access_name)[access_name]
                        cookie_data = data['Cookies']
                        tokenV3 = data['WBTokenV3']
                        self.__logger.debug("Получение cookies")


                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return cookie_data, tokenV3
        
        def fill_tokenv3_access_data(self):
                """ Заполняет словарь доступов к БД """
                tokenV3 = {}
                try:
                        tokenV3 = self.__sec_mod.get_access_data('Wildberries Seller ЛК Cookies с доступом Поставки')['Wildberries Seller ЛК Cookies с доступом Поставки']['WBTokenV3']

                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return tokenV3
        
        def fill_api_key_access_data(self):
                """ Заполняет словарь доступов к БД """
                wb_analytics_api_key = {}
                try:    
                        data = self.__sec_mod.get_access_data('Wildberries API для контента')
                        wb_analytics_api_key = data['Wildberries API для контента']

                except Exception as e:
                        self.__logger.exception(f"Ошибка при получение доступов к БД: {e}")

                return wb_analytics_api_key



        def get_mysql_connect_params_dict(self):

                return self.__mysql_connect_params_dict
        
        
        def get_cookies(self):
                return self.__cookies
        
        def get_tokenV3(self):
                return self.__tokenV3
        
        def get_wb_analytics_api_key(self):
                return self.__wb_analytics_api_key
        

