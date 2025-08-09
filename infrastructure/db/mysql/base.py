import os
import logging
import time
import asyncio
import aiomysql
import pymysql
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ================================
# Async (aiomysql)
# ================================

class AsyncDatabase:
    def __init__(self):
        self.pool = None

    async def connect(self, retries: int = 5, delay: int = 2):
        if self.pool is not None:
            return self.pool

        for attempt in range(1, retries + 1):
            try:
                self.pool = await aiomysql.create_pool(
                    host=os.getenv('NEZKA_DB_MYSQL_HOST'),
                    port=int(os.getenv('NEZKA_DB_MYSQL_PORT')),
                    user=os.getenv('NEZKA_DB_MYSQL_USER'),
                    password=os.getenv('NEZKA_DB_MYSQL_PASSWORD'),
                    db=os.getenv('NEZKA_DB_MYSQL_DB'),
                    autocommit=True)
                
                logging.info("Successfully connected to async MySQL DB.")
                return self.pool
            except Exception as e:
                logging.warning(f"[{attempt}/{retries}] Async MySQL connection failed: {e}")
                await asyncio.sleep(delay)

        raise ConnectionError("Failed to connect to async MySQL DB after multiple attempts.")
    
    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def execute_query(self, query, params=None):
        pool = await self.connect()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, params)
                    result = await cursor.fetchall()
                    return result
                except Exception as e:
                    logging.error(f"Async execute_query error: {e}")
                    raise

    async def execute_scalar(self, query, params=None):
        result = await self.execute_query(query, params)
        return result[0][0] if result else None

    async def execute_non_query(self, query, params=None):
        pool = await self.connect()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, params)
                except Exception as e:
                    logging.error(f"Async execute_non_query error: {e}")
                    raise

    async def execute_many(self, query, param_list):
        pool = await self.connect()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.executemany(query, param_list)
                except Exception as e:
                    logging.error(f"Async execute_many error: {e}")
                    raise

# ================================
# Sync (pymysql)
# ================================

class SyncDatabase:
    def __init__(self, host, port, user, password, db):
        self.connection = None
        self.db_params = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "db": db,
            "autocommit": True,
            "cursorclass": pymysql.cursors.DictCursor
        }

    def connect(self, retries=5, delay=2):
        if self.connection is not None:
            return self.connection

        for attempt in range(1, retries + 1):
            try:
                self.connection = pymysql.connect(**self.db_params)
                return self.connection
            except Exception as e:
                logging.error(f"[{attempt}/{retries}] Failed to connect to MySQL DB: {e}")
                time.sleep(delay)

        raise ConnectionError("Failed to connect to MySQL DB after multiple attempts.")

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, params=None):
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Sync execute_query error: {e}")
            raise

    def execute_scalar(self, query, params=None):
        result = self.execute_query(query, params)
        return list(result[0].values())[0] if result else None

    def execute_non_query(self, query, params=None):
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
        except Exception as e:
            logging.error(f"Sync execute_non_query error: {e}")
            self.connection.rollback()
            raise

    def execute_many(self, query, param_list):
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.executemany(query, param_list)
        except Exception as e:
            logging.error(f"Sync execute_many error: {e}")
            self.connection.rollback()
            raise

