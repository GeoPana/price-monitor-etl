from __future__ import annotations

"""Central logging configuration used by the CLI and runtime."""

import logging
import logging.config
from pathlib import Path


def configure_logging(log_level: str, log_file: Path) -> None:
    """Configure console and file logging with a shared formatter."""

    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": log_level,
                    "formatter": "standard",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "level": log_level,
                    "formatter": "standard",
                    "filename": str(log_file),
                    "encoding": "utf-8",
                },
            },
            "root": {
                "level": log_level,
                "handlers": ["console", "file"],
            },
        }
    )

    logging.getLogger(__name__).debug("Logging configured.")
