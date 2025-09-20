import os
import logging
import time
import asyncio
import aiomysql
import pymysql
from dotenv import load_dotenv
import queue
import threading

load_dotenv()
logging.basicConfig(level=logging.INFO)


class ConnectionPool:
    def __init__(self, maxsize=10, **db_params):
        self._pool = queue.Queue(maxsize)
        self._db_params = db_params
        self._lock = threading.Lock()

        for _ in range(maxsize):
            self._pool.put(self._create_connection())

    def _create_connection(self):
        return pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            **self._db_params)

    def get_connection(self, timeout=5):
        try:
            conn = self._pool.get(timeout=timeout)
            if not conn.open:
                conn = self._create_connection()
            return conn
        except queue.Empty:
            raise ConnectionError(f"Нет свободных соединений в пуле (maxsize={self._pool.maxsize})")


    def return_connection(self, conn):
        try:
            if conn.open:
                self._pool.put(conn)
            else:
                self._pool.put(self._create_connection())
        except Exception:
            self._pool.put(self._create_connection())

    def close_all(self):
        while not self._pool.empty():
            conn = self._pool.get()
            try:
                conn.close()
            except Exception:
                pass



class SyncDatabase:
    def __init__(self, host, port, user, password, db, maxsize=10):
        self.pool = ConnectionPool(
            host=host,
            port=int(port),
            user=user,
            password=password,
            db=db,
            maxsize=maxsize)

    def execute_query(self, query, params=None):
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Sync execute_query error: {e}")
            raise
        finally:
            self.pool.return_connection(conn)

    def execute_scalar(self, query, params=None):
        result = self.execute_query(query, params)
        return list(result[0].values())[0] if result else None

    def execute_non_query(self, query, params=None):
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
        except Exception as e:
            logging.error(f"Sync execute_non_query error: {e}")
            conn.rollback()
            raise
        finally:
            self.pool.return_connection(conn)

    def execute_many(self, query, param_list):
        conn = self.pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(query, param_list)
        except Exception as e:
            logging.error(f"Sync execute_many error: {e}")
            conn.rollback()
            raise
        finally:
            self.pool.return_connection(conn)