import logging
import os
from logging.handlers import RotatingFileHandler

os.makedirs("logs", exist_ok=True)

def _create_logger(name: str, logfile: str, max_bytes: int = 5_000_000, backup_count: int = 5):
    """
    Internal function untuk membuat logger dengan rotating file handler.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # File handler (rotating biar gak bengkak)
        file_handler = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count)
        stream_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


logger_extract = _create_logger("Extract", "logs/extract.log")
logger_transform = _create_logger("Transform", "logs/transform.log")
logger_load = _create_logger("Load", "logs/load.log")

