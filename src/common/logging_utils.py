"""Structured logging for Lambdas and jobs.

Why JSON logs:
    `print("low stock")` is readable by a person but hard to search. A JSON line such as
    {"level": "WARNING", "message": "low stock", "part": "BATTERY-A12"} can be filtered and
    aggregated with CloudWatch Logs Insights (e.g. "count low-stock alerts per part").
"""

from __future__ import annotations

import json
import logging
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formats every log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        """Turn a log record into a JSON string.

        Args:
            record: The standard logging record.

        Returns:
            One JSON line with level, logger name, message and any extra fields.
        """
        # Base fields every log line has
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Extra business context passed as logger.info("...", extra={"fields": {...}})
        payload.update(getattr(record, "fields", {}))

        # default=str keeps non-JSON types (dates, decimals) from breaking the logger
        return json.dumps(payload, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that writes JSON lines.

    Args:
        name: Usually __name__ of the calling module, so logs show where they came from.
        level: Minimum level to emit.

    Returns:
        A configured logger. Calling this twice with the same name reuses the same handler.
    """
    logger = logging.getLogger(name)

    # Attach the JSON handler only once, even if the module is imported many times
    # (Lambda reuses the same Python process across invocations)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    logger.setLevel(level)

    # Don't also send records to the root logger, or each line would print twice
    logger.propagate = False
    return logger
