"""Structured logging for local experiments and future services."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TextIO

from trading.configuration import LoggingSettings


class JsonFormatter(logging.Formatter):
    """Emit one UTC JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize stable fields without logging configuration or credentials."""
        payload: dict[str, str] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: LoggingSettings, stream: TextIO | None = None) -> None:
    """Configure the project's logger while leaving unrelated loggers untouched."""
    logger = logging.getLogger("trading")
    logger.setLevel(settings.level)
    logger.propagate = False
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    handler = logging.StreamHandler(stream or sys.stderr)
    formatter = (
        JsonFormatter()
        if settings.json_output
        else logging.Formatter("%(levelname)s %(name)s: %(message)s")
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
