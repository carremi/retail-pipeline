"""Centralized logging configuration: console + rotating file."""
import logging
import sys
from logging.handlers import RotatingFileHandler

from retail_pipeline.utils.config import config


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured to write to console and rotating file."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file (5 files of 2 MB)
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(
        config.LOGS_DIR / "pipeline.log",
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False
    return logger
