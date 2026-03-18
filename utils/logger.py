"""Logging utility — console + rotating file handler."""
import logging
import os
from datetime import datetime


def get_logger(name: str, log_dir: str = None, level=logging.INFO) -> logging.Logger:
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        fh = logging.FileHandler(
            os.path.join(log_dir, f"pipeline_{datetime.now():%Y-%m-%d}.log")
        )
        fh.setFormatter(fmt)
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
