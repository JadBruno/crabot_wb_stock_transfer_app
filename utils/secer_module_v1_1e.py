# Запись 3-го ключа в системное окружение для дальнейшей работы модуля
# Windows: в командной строке набрать setx encryption_key_3 fhsdgfjshdkfjshdk
# Linux: в файле /etc/profile добавить запись encryption_key_3='fhsdgfjshdkfjshdk'

# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.primitives import padding
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


import os
import base64
# from cryptography.hazmat.primitives.ciphers.algorithms import AES as AES_hazmat
from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Protocol.KDF import PBKDF2
import pymysql
import os
import traceback
import pymysql.cursors
import pymysql
import random
import string
import base64
import json
import sys
from utils.logger import get_logger

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Для импорта из родительской директории
sys.path.insert(0, parent_dir)

from csd import encryption_key_1, encryption_key_3_name, encryption_level, max_data_size, delimiter,mySQLConnectParam_dostup
delimiter = bytes(delimiter.encode('utf-8'))

class SecurityModule:
    # -------------------------------------------------------------------------------------------------------------
    # -------------------------------------- ОСНОВНЫЕ ФУНКЦИИ -----------------------------------------------------
    # -------------------------------------------------------------------------------------------------------------
    def __init__(self):
        """
        Инициализация SecurityModule.
        
        Загружает ключи шифрования и данные для подключения к базе данных.
        """
        try:
            self.BLOCK_SIZE = 16 # Размер блока для шифрования
            self.mySQLConnectParam = mySQLConnectParam_dostup
            self.characters = string.ascii_letters + string.punctuation + string.digits
            self.encryption_key_1 = encryption_key_1 # Первый ключ берем из crabot_data_settings
            self.encryption_key_2 = self.get_encryption_key_from_db() # Второй ключ достается из БД
            self.encryption_key_3 = os.environ.get(encryption_key_3_name) # Третий ключ - переменная системного окружения
            self.encryption_key_list = [self.encryption_key_1,self.encryption_key_2,self.encryption_key_3] # Cобрали лист из ключей, чтобы удобнее его передавать
            self.encryption_level = encryption_level # Установили количество уровней шифрования
            salt_iv_dict = self.get_default_salt_and_iv() # Скачали дефолтную соль и iv из БД, чтобы шифровать/дешифровать названия модулей
            self.default_salt = salt_iv_dict['salt']
            self.default_iv = salt_iv_dict['iv']
            self.logger = get_logger(__name__)
            
        #    self.logger.debug(f'Соль {self.default_salt}. Длина: {len(self.default_salt)}')
        #    self.logger.debug(f'IV {self.default_iv}. Длина {len(self.default_iv)}')
        #    self.logger.debug(f'Ключ_1 длина {"больше" if len(self.encryption_key_1) > 10 else "="} {10 if len(self.encryption_key_1) > 10 else len(self.encryption_key_1)}')
        #    self.logger.debug(f'Ключ_2 длина {"больше" if len(self.encryption_key_2) > 10 else "="} {10 if len(self.encryption_key_2) > 10 else len(self.encryption_key_2)}')
        #    self.logger.debug(f'Ключ_3 длина {"больше" if len(self.encryption_key_3) > 10 else "="} {10 if len(self.encryption_key_3) > 10 else len(self.encryption_key_3)}')

        except Exception as ex:
            self.logger.debug(f"{type(ex).__name__}: {ex}\n{traceback.format_exc()}")

    def save_access_data(self, access_data_dict):
        """  Метод сохранияем зашифрованные данные в нашей БД
            Вводные данные: словарь с чувствительной информацией
        """
        service_name = tuple(access_data_dict.keys())[0] # Отделяем от всей информации название сервиса
        encrypted_service_name = self.encrypt_service_name(service_name) # Шифруем наш сервис дефолтной солью и iv
        encrypted_entries_list = [] # Cловарь с листами, где кадый словарь зашифрованные key, value access_data_dict
        for key, value in access_data_dict[service_name].items(): 
            key = str(key)
            key = self.data_randomize(key)
            value = str(value)
            value = self.data_randomize(value) # Рандомизируем значение value
            access_data_name = self.encrypt(key) # Шифруем значение key
            access_data_value = self.encrypt(value) # Шифруем значение value
            encrypted_entries_list.append({
                'service_name':encrypted_service_name,
                'access_data_name':access_data_name,
                'access_data_value':access_data_value
                                      }) # Cоздаем новый словарь
       
        # Загрузка зашифрованных данных в БД
        connection = pymysql.connect(**self.mySQLConnectParam,
                                cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            delete_query = """DELETE FROM u_access_data WHERE `service_name` = %(service_name)s"""
            sql_parameters_delete = {'service_name':encrypted_service_name}
            cursor.execute(delete_query, sql_parameters_delete)

            insert_query = """INSERT INTO u_access_data (`service_name`,`access_data_name`,`access_data_value`) 
                            VALUES (%(service_name)s,%(access_data_name)s,%(access_data_value)s);"""
            for row in encrypted_entries_list:
                sql_parameters_insert = {'service_name':row['service_name'],
                                  'access_data_name':row['access_data_name'],
                                  'access_data_value':row['access_data_value']}
                cursor.execute(insert_query, sql_parameters_insert)
        connection.commit()
        self.logger.debug('Зашифрованные данные загружены в БД')

    def get_access_data(self,service_name):
        """ Получаем расшифрованные значения из БД по названию сервиса
            Вводные данные: service_name - названия сервиса
        """
        def get_encrypted_access_data(service_name):
            """Получаем данные из БД в зашифрованном виде
                Вводные данные: service_name - названия сервиса
                Вывод: encrypted_access_data - зашифрованные значения из БД"""
            connection = pymysql.connect(**self.mySQLConnectParam,
                                             cursorclass=pymysql.cursors.DictCursor)
            with connection.cursor() as cursor:
                placeholder = {'service_name':service_name}
                query = "SELECT `access_data_name`,`access_data_value` FROM u_access_data where `service_name` = %(service_name)s"
                cursor.execute(query,placeholder)
                encrypted_access_data = cursor.fetchall()
                return encrypted_access_data
        
        encrypted_service_name = self.encrypt_service_name(service_name) # Шифруем имя сервиса при помощи стандартных соли и iv
        encrypted_access_data = get_encrypted_access_data(encrypted_service_name) # Получаем данные из БД при помощи зашифрованного имено сервиса
        access_data = {service_name:{}} # Наш итоговый словарик
        for single_access_dict in encrypted_access_data:
            encrypted_access_data_name, encrypted_access_data_value = single_access_dict.values() # берем только значения колонок без их имен
            delimiter = b'$$$' # Делимитер, чтобы достать уникальную соль и iv для создания приватных ключей дешифровки
            access_data_name_salt, access_data_name_hash, access_data_name_iv = encrypted_access_data_name.split(delimiter) # Получаем наши соль, хеш и iv с именем значения
            randomized_access_data_name = self.decrypt(encrypted_string=access_data_name_hash,salt=access_data_name_salt, iv=access_data_name_iv) # Дешифруем
            randomized_access_data_name = randomized_access_data_name.decode('utf-8') # Рекодируем значение в 'utf-8
            decrypted_access_data_name = self.data_derandomize(randomized_access_data_name)
            access_data_value_salt, access_data_value_hash, access_data_value_iv = encrypted_access_data_value.split(delimiter) # Получаем наши соль, хеш и iv для значения
            randomized_access_data_value = self.decrypt(encrypted_string=access_data_value_hash,salt=access_data_value_salt, iv=access_data_value_iv) # Дешифруем
            randomized_access_data_value = randomized_access_data_value.decode('utf-8') # Рекодируем значение в 'utf-8
            decrypted_access_data_value = self.data_derandomize(randomized_access_data_value) # Дерандомизируем
            access_data[service_name][decrypted_access_data_name] = decrypted_access_data_value # Записываем в словарик на вывод
        return access_data # Я знаю, ты попадешь в надежные руки
    




    # -------------------------------------------------------------------------------------------------------------
    # --------------------------------------ВСПОМОГАТЕЛЬНЫЕ ПРИВАТНЫЕ ФУНКЦИИ--------------------------------------
    # -------------------------------------------------------------------------------------------------------------
    # Скачивание encryption_key_2 из БД
    def get_encryption_key_from_db(self): 
        connection = pymysql.connect(**self.mySQLConnectParam)
        with connection.cursor() as cursor:
            query = "SELECT `value` FROM u_last_of_us"
            cursor.execute(query)
            encryption_key = cursor.fetchone()
        return encryption_key[0]

    # Скачивание Соль и IV для шифровки-расшифровки названия из БД
    def get_default_salt_and_iv(self): 
        connection = pymysql.connect(**self.mySQLConnectParam)
        with connection.cursor() as cursor:
            query = "SELECT `salt`,`iv` FROM u_access_data_extra"
            cursor.execute(query)
            salt, iv = cursor.fetchone()
            salt_iv_dict = {"salt": salt, "iv": iv}
        return salt_iv_dict

    def generate_random_string(self, length):
        """
        Генерирует случайную строку заданной длины
        Args:      length (int): Длина строки.

        Returns:   str: Сгенерированная строка.
        """
        try:
            random_str = ''
            for _ in range(length):
                random_str += random.choice(self.characters)
            return random_str 
        except Exception as ex:
            self.logger.debug(f"{type(ex).__name__}: {ex}\n{traceback.format_exc()}")
            return None

    def data_randomize(self,data):
        """
        Формирует строку для шифрования с данными и паролем.
        Args:       data (str): данные для шифрования
        Returns:    str: усложненная строка для дальнейшего шифрования
        """
        try:
            shift_index = 50 # Индекс смещения строки, должен быть ниже 100(минимальный размер зашифрованной строки)  
            max_data_size_length = len(str(max_data_size))
            
            data_length = len(data)
            data_length_str = str(data_length).zfill(max_data_size_length) #Дописываем слева 0 к числу-длине строки, чтобы было 3 знака, например, 013

            len_data_length = len(str(data_length))
            # Минимальная длина случайной строки 1000 для данных в 1 символ и ~в 100 раз больше длины данных в остальных случаях, 
            if len_data_length == 1:
                encrypted_string_size = int("1"+"0"*(len_data_length+2))
            else:
                encrypted_string_size = int("1"+"0"*(len_data_length+1))
            noise_str = self.generate_random_string(encrypted_string_size-data_length-(max_data_size_length*2-2))
            # noise_str = self.generate_random_string(1000+data_length*2)
            # Генерация случайных символов
            split_index_coorditate_start = round(shift_index+data_length*0.5)
            if len(str(data_length)) > 1:
                split_index_coorditate_end = round(encrypted_string_size-split_index_coorditate_start)
            else:
                split_index_coorditate_end = round(encrypted_string_size-split_index_coorditate_start+data_length)
            split_index = random.randint(split_index_coorditate_start, split_index_coorditate_end)
            split_index_str = str(split_index).zfill(max_data_size_length)
            noise_str_first_part, noise_str_second_part = noise_str[0:split_index], noise_str[split_index:]

            # Прописываем адрес пароля, перемешивая его с рандомными буквами и знаками
            address = random.choice(self.characters)
            for number in data_length_str:
                address += f"{number}{random.choice(self.characters)}"
            address += random.choice(self.characters)
            for number in split_index_str:
                address += f"{number}{random.choice(self.characters)}"

        #    self.logger.debug(f'Длина адреса: {len(address)}')
            
            encrypted_string = address+noise_str_first_part + data + noise_str_second_part
            return encrypted_string
        except Exception as ex:
            self.logger.debug(f"{type(ex).__name__}: {ex}\n{traceback.format_exc()}")
            return None
            
    def get_private_key(self, key, salt):
        """ Получаем приватный ключ PBKDF2 для усложнения шифрования при помощи псевдорандома            
            Args:       key - оригинальный ключ, 
                        salt - соль которую мы будем подмешивать в ключ

            Returns:    key - сгенерированный псевдорандомный ключ, которым будем проводить AES шифрование
        """
        kdf = PBKDF2(key, salt, 64, 1000)
        private_key = kdf[:32] # Соответствует алгоритму AES-256
        return private_key

    def add_string_pad(self, string_to_encrypt):
        """ Добавляет подставку для строки, чтобы она стала кратной размеру блока шифрования
            Вводные данные: string_to_encrypt - строка для шифрования
            Вывод:          padded_string - строка с подставкой
        """
        string_need_bytes_count = self.BLOCK_SIZE - len(string_to_encrypt) % self.BLOCK_SIZE
        padded_string = string_to_encrypt + string_need_bytes_count * bytes([string_need_bytes_count])
        return padded_string

    def aes_encrypt(self, string_to_encrypt, key, salt, iv):
        """ Шифрует входные данные по алгоритму AES
            Вводные данные: string_to_encrypt - строка для шифрования
                            key - наш изначальный ключ в незашифрованном виде
                            salt - соль для шифрования
                            iv - iv для шифрования
            Вывод: encrypted_string - зашифрованная строка
        """
        private_key = self.get_private_key(key,salt) # Cоздаем приватный ключ
        padded_string_to_encrypt = self.add_string_pad(string_to_encrypt) # Ставим строку на подставку (добавляем байтов, если на хватает для шифрования)
        cipher = AES.new(private_key, AES.MODE_CBC, iv) # Шифруем по протоколу AES в режиме CBC
        encrypted_string = base64.b64encode(cipher.encrypt(padded_string_to_encrypt)) # Получаем зашифрованную строку в виде hash x64
        return encrypted_string

    # def aes_hazmat_encrypt(self, string_to_encrypt, key, salt, iv):
    #     """ Шифрует входные данные по алгоритму AES
    #         Вводные данные: string_to_encrypt - строка для шифрования
    #                         key - наш изначальный ключ в незашифрованном виде
    #                         salt - соль для шифрования
    #                         iv - iv для шифрования
    #         Вывод: encrypted_string - зашифрованная строка
    #     """
    #     private_key = self.get_private_key(key,salt) # Cоздаем приватный ключ
    #     padded_string_to_encrypt = self.add_string_pad(string_to_encrypt) # Ставим строку на подставку (добавляем байтов, если на хватает для шифрования)
    #     cipher = Cipher(algorithms.AES(private_key), modes.CBC(iv), backend=default_backend())
    #     encryptor = cipher.encryptor()
    #     encrypted_data  = encryptor.update(padded_string_to_encrypt) + encryptor.finalize()
    #     return encrypted_data

    def encrypt(self,string_to_encrypt, salt=None,iv=None):
        """ Шифрует наши переменные типа str
            Args:       string_to_encrypt - строка для шифрования
                        salt - можно ввести свою соль
                        iv - можно ввесть свой iv 

            Returns:    encrypted_string - зашифрованная строка в виде hash типа bytes
        """
        string_to_encrypt = string_to_encrypt.encode() # Перевели строку в bytes
        default_string_to_encrypt = string_to_encrypt # Строка на случай ошибки генерации, перезапустит процесс
        if salt == None: # Если не указана пользовательская соль, генерируем свою
            salt = os.urandom(16)
            if delimiter in salt:
                for _ in range(2):
                    salt = os.urandom(16)
                    if delimiter not in salt:
                        break
                    else:
                        self.logger.debug('Ошибка генерации salt из-за делимитера')
            

        if iv == None: # Если не указан пользовательский iv, генерируем свой
            iv = os.urandom(16)
            if delimiter in iv:
                for _ in range(10):
                    iv = os.urandom(16)
                    if delimiter not in iv:
                        break
                    else:
                        self.logger.debug('Ошибка генерации iv из-за делимитера')


        # self.logger.debug('Соль',salt)
        # self.logger.debug('iv',iv)
        for _ in range(3):
            for _ in range(self.encryption_level): # Уровень шифрования
                for key in self.encryption_key_list: # Наши ключи
                    string_to_encrypt = self.aes_encrypt(string_to_encrypt, key, salt, iv) # Шифруем строку много раз
            if delimiter not in string_to_encrypt:
                break
            else:
                salt = os.urandom(16)
                string_to_encrypt = default_string_to_encrypt
                self.logger.debug('Делимитер найден в конечном хеше. Повторная генерация соли')
        
        encrypted_string = salt + delimiter + string_to_encrypt + delimiter + iv # salt$$$encrypyted_string$$$iv
        return encrypted_string
    
    def aes_decrypt(self, string_to_decrypt, key, salt, iv):
        """ Дешифруем хеш:
            Вводные данные: encrypted_string - зашифрованная строка
                            key - оригинальный ключ шифрования            
        """
        unpad = lambda s: s[:-ord(s[len(s) - 1:])] # Снимает строку с подставки (убираем лишние байты, количество которых указано в последнем байте)
        private_key = self.get_private_key(key, salt) # Воссоздаем приватный ключ шифрования на основе нашего ключа и соли
        string_to_decrypt = base64.b64decode(string_to_decrypt) # декодируем хеш из x64
        cipher = AES.new(private_key, AES.MODE_CBC, iv) # Дешифруем по протоколу AES в режиме CBC
        return unpad(cipher.decrypt(string_to_decrypt)) # снимаем строку с подставки (убираем лишние байты, количество которых указано в последнем байте)

    def decrypt(self,encrypted_string,salt, iv):
        """ Дешифруем строку:
            Вводные данные: encrypted_string - hash с чувствительной информацией
                            salt - соль для воссоздания приватного ключа
                            iv - iv
            Вывод: string_to_decrypt - расшифрованная информация в переменной типа bytes
        """
        string_to_decrypt = encrypted_string # наш хеш
        
        for _ in range(self.encryption_level): # Уровень шифрования
            for key in reversed(self.encryption_key_list): # Для всех наших ключей из листа
                string_to_decrypt = self.aes_decrypt(string_to_decrypt, key, salt, iv) # Дешифруем
        return string_to_decrypt
    
    def encrypt_service_name(self,service_name):
        """ Шифруем название сервиса.
            Вводные данные: service_name - название сервиса
            Вывод: encrypted_service_name - зашифрованное название сервиса
        """
        encrypted_service_name = self.encrypt(service_name,self.default_salt, self.default_iv).split(delimiter)[1] # Шифруем при помощи encrypt, дефолтной соли и iv
        return encrypted_service_name # Возвращаем только хеш без соли и iv
    
    def data_derandomize(self,randomized_string):
        """ Дерандомизируем значение строки с чувствительной информацией
            Вводные данные: randomized_string - наша рандомизированая строка
            Вывод: derandomized_string - наша чувствительная информация
        """
        max_data_size_length = len(str(max_data_size))
        address_size = max_data_size_length*4+2
        data_length_str = ""
        data_address_str = ""
        for step in range(max_data_size_length):
            data_length_str += randomized_string[(step+1)*2-1] # Берет 1, 3, 5, ... символы из усложненной строки для определения длины пароля
            data_address_str += randomized_string[(step+max_data_size_length+1)*2] # Берет max_data_size_length*2 + 1, 3, 5, ... символы из усложненной строки для определения адреса пароля
        data_length = int(data_length_str)
        data_address = int(data_address_str)
        derandomized_string = randomized_string[data_address+address_size:data_address+data_length+address_size] # Находим чувствительную информацию
        return derandomized_string

    