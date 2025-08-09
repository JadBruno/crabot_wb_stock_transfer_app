import logging
import sys

LOG_LEVEL = 'INFO'

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
