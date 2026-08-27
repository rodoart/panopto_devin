"""Módulo logging con las funciones setup_logging, get_logger."""

import logging
import logging.config
import os

DEFAULT_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def setup_logging(level: str = None) -> None:
    """Función que configura logging."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    level = (level or os.getenv("MECV_LOG_LEVEL", "INFO")).upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        level = "INFO"

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": DEFAULT_FORMAT,
                "datefmt": DEFAULT_DATEFMT,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "mecv": {
                "level": level,
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }
    logging.config.dictConfig(config)


def get_logger(name: str) -> logging.Logger:
    """Función que obtiene logger."""
    setup_logging()
    return logging.getLogger(name)
