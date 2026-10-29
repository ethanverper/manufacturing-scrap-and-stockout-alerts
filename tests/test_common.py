"""Unit tests for the shared helpers. They run offline: no AWS, no credentials."""

from __future__ import annotations

import json
import logging

import pytest

from src.common import config
from src.common.logging_utils import JsonFormatter


def test_get_str_returns_value_or_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """A set variable wins over the default; an unset one falls back to it."""
    # Set one variable and leave another unset
    monkeypatch.setenv("TOPIC", "arn:aws:sns:us-east-1:111122223333:alerts")
    monkeypatch.delenv("MISSING", raising=False)

    assert config.get_str("TOPIC") == "arn:aws:sns:us-east-1:111122223333:alerts"
    assert config.get_str("MISSING", "fallback") == "fallback"


def test_get_str_fails_fast_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """A required setting that is missing must stop the program with a clear error."""
    monkeypatch.delenv("REQUIRED", raising=False)

    with pytest.raises(KeyError, match="REQUIRED"):
        config.get_str("REQUIRED")


def test_numeric_settings_are_converted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment values are text; thresholds must come back as numbers."""
    monkeypatch.setenv("SAFETY_STOCK_UNITS", "50")
    monkeypatch.setenv("REJECT_RATE_PCT", "3.5")

    assert config.get_int("SAFETY_STOCK_UNITS", 10) == 50
    assert config.get_float("REJECT_RATE_PCT", 1.0) == 3.5


def test_json_formatter_includes_business_fields() -> None:
    """Extra fields must land in the JSON line, so Logs Insights can filter by them."""
    # Build a record the way logger.info("...", extra={"fields": {...}}) would
    record = logging.LogRecord("alerts", logging.WARNING, __file__, 1, "low stock", None, None)
    record.fields = {"part": "BATTERY-A12", "stock_left": 47}

    line = json.loads(JsonFormatter().format(record))

    assert line == {
        "level": "WARNING",
        "logger": "alerts",
        "message": "low stock",
        "part": "BATTERY-A12",
        "stock_left": 47,
    }
