"""Structured log format checks."""

import io
import json
import logging

from trading.configuration import LoggingSettings
from trading.logging_setup import configure_logging


def test_json_logging_uses_utc_and_no_duplicate_handlers() -> None:
    """Reconfiguration emits one parseable event per message."""
    stream = io.StringIO()
    settings = LoggingSettings(level="INFO", json_output=True)
    configure_logging(settings, stream)
    configure_logging(settings, stream)
    logging.getLogger("trading.test").info("ready")
    events = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert len(events) == 1
    assert events[0]["message"] == "ready"
    assert events[0]["timestamp"].endswith("+00:00")
