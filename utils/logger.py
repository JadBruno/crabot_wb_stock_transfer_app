import logging
import sys
import asyncio
from functools import wraps

LOG_LEVEL = 'DEBUG'

def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)
    level = getattr(logging, LOG_LEVEL, logging.DEBUG)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger

def simple_logger(func=None, *, logger_name: str = "app"):
    def decorator(func):
        logger = get_logger(logger_name)

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                logger.debug(f'Запускаем функцию {func.__name__}()')
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    logger.exception(f"Ошибка в {func.__name__}():\n {e}")
                    raise
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                logger.debug(f'Запускаем функцию {func.__name__}()')
                try:
                    return func(self, *args, **kwargs)
                except Exception as e:
                    logger.exception(f"Ошибка в {func.__name__}():\n {e}")
                    raise
            return sync_wrapper

    if func is None:
        # будет вызван как @simple_logger(logger_name="...")
        return decorator
    else:
        # будет вызван как @simple_logger
        return decorator(func)

