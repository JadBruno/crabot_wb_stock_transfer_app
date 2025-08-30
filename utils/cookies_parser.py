import base64
import hmac
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from requests.cookies import RequestsCookieJar
from datetime import datetime
import time
from typing import Union
from utils.logger import get_logger


class CookieDecryptor:
    def __init__(self, key_base64: str):
        key_bytes = base64.b64decode(key_base64)
        if len(key_bytes) != 32:
            raise ValueError("Неверный ключ: должен быть 32 байта после base64-декодирования")

        self.enc_key = key_bytes[:16]
        self.hmac_key = key_bytes[16:]

    def decrypt(self, token: str) -> str:
        try:
            data = base64.b64decode(token)

            if len(data) < 57:
                raise ValueError("Токен слишком короткий, не соответствует формату")

            version = data[0]
            timestamp = data[1:9]  # 8 байт
            iv = data[9:25]        # 16 байт
            hmac_start = len(data) - 32
            ciphertext = data[25:hmac_start]
            hmac_received = data[hmac_start:]

            # Проверим подпись
            message = data[:hmac_start]
            hmac_calculated = hmac.new(self.hmac_key, message, hashlib.sha256).digest()
            if not hmac.compare_digest(hmac_calculated, hmac_received):
                raise ValueError("HMAC-подпись недействительна")

            # Расшифровываем
            cipher = AES.new(self.enc_key, AES.MODE_CBC, iv)
            decrypted = unpad(cipher.decrypt(ciphertext), AES.block_size)
            return decrypted.decode("utf-8")

        except Exception as e:
            raise ValueError(f"Ошибка при расшифровке: {e}")
        
    @staticmethod
    def parse_cookie_string(cookie_string: str) -> RequestsCookieJar:
        jar = RequestsCookieJar()
        

        lines = cookie_string.strip().splitlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Определим разделитель
            if '\t' in line:
                parts = line.split('\t')
            else:
                parts = line.split()

            if len(parts) < 2:
                continue

            name = parts[0].strip()

            if name == "WBTokenV3":
                continue
        
            value = parts[1].strip()
            domain = parts[2] if len(parts) > 2 else ""
            path = parts[3] if len(parts) > 3 else "/"
            expires_raw = parts[4] if len(parts) > 4 else None

            # Обработка даты истечения
            expires = None
            if expires_raw:
                try:
                    dt = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
                    expires = int(time.mktime(dt.timetuple()))
                except Exception:
                    expires = None  # если формат странный — не ставим

            secure = "✓" in parts or "secure" in line.lower()
            rest = {"HttpOnly": "httponly" in line.lower()}  # можно добавить больше флагов при необходимости

            # Добавим cookie в jar
            jar.set(
                name=name,
                value=value,
                domain=domain.lstrip("."),  # requests не любит точки в начале
                path=path,
                expires=expires,
                secure=secure,
                rest=rest)
            

        return jar
    

    @staticmethod
    def extract_tokenV3(cookie_string: str) -> Union[str, None]:

        lines = cookie_string.strip().splitlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Определим разделитель
            if '\t' in line:
                parts = line.split('\t')
            else:
                parts = line.split()

            if len(parts) < 2:
                continue

            name = parts[0].strip()
            value = parts[1].strip()

            if name == "WBTokenV3":
                return value
        
