"""Validation of incoming records: good scans pass, every kind of broken scan is caught with a clear reason."""

from __future__ import annotations

import base64
import json
from datetime import date, timezone

import pytest

from src.generators.live import build_live_events
from src.lambdas.realtime_alerts.parsing import InvalidEventError, decode_kinesis_record, parse_event

# A valid REJECTED scan, used as the base for the broken variants below
VALID_REJECT = {
    "event_id": "EV-20261028-MTY-0000123",
    "event_time": "2026-10-28T13:15:42Z",
    "event_type": "REJECTED",
    "plant": "PLANT-MTY",
    "line": "LINE-2",
    "station": "LINE-2-ASSEMBLY-3",
    "part": "BATTERY-A12",
    "supplier": "SUP-VOLTEX",
    "lot": "LOT-7781",
    "quantity": 2,
    "reason_code": "SUPPLIER_DEFECT",
}


def kinesis_record(payload: dict | bytes) -> dict:
    """Wrap a payload the way Lambda receives it from Kinesis (base64 under kinesis.data)."""
    data = payload if isinstance(payload, bytes) else (json.dumps(payload) + "\n").encode()
    return {"kinesis": {"data": base64.b64encode(data).decode(), "sequenceNumber": "1"}}


def test_valid_reject_is_parsed_with_its_derived_values() -> None:
    """A good scan becomes a typed event with the keys and deltas the rest of the Lambda needs."""
    event = parse_event(decode_kinesis_record(kinesis_record(VALID_REJECT)))

    assert event.event_time.tzinfo == timezone.utc
    assert event.stock_delta == -2  # rejected parts leave the shelf
    assert event.hour_bucket == "2026-10-28T13"
    assert event.plant_part == "PLANT-MTY#BATTERY-A12"
    assert event.plant_station == "PLANT-MTY#LINE-2-ASSEMBLY-3"
    assert event.counts_for_reject_rate


@pytest.mark.parametrize(
    ("change", "expected_message"),
    [
        ({"event_type": "SCRAPPED"}, "Unknown event_type"),
        ({"plant": None}, "Missing required fields"),
        ({"quantity": -2}, "positive integer"),
        ({"quantity": "2"}, "positive integer"),
        ({"quantity": True}, "positive integer"),
        ({"reason_code": None}, "Missing required fields"),
        ({"reason_code": "BAD_LUCK"}, "Invalid reason_code"),
        ({"event_time": "2026-10-28 13:15:42"}, "event_time not in"),
    ],
)
def test_broken_scans_are_rejected_with_a_reason(change: dict, expected_message: str) -> None:
    """Each corruption a real scanner can produce is caught, and the message names the problem."""
    with pytest.raises(InvalidEventError, match=expected_message):
        parse_event({**VALID_REJECT, **change})


def test_reason_code_only_allowed_on_rejects() -> None:
    """A CONSUMED event with a reason code is contradictory, so it is refused."""
    consumed = {**VALID_REJECT, "event_type": "CONSUMED"}
    with pytest.raises(InvalidEventError, match="only allowed on REJECTED"):
        parse_event(consumed)


def test_undecodable_records_are_rejected() -> None:
    """Garbage bytes and non-object JSON never reach the business rules."""
    with pytest.raises(InvalidEventError, match="Undecodable"):
        decode_kinesis_record(kinesis_record(b"{not json"))
    with pytest.raises(InvalidEventError, match="not a JSON object"):
        decode_kinesis_record(kinesis_record(b"[1, 2, 3]"))


def test_every_generated_live_event_is_classified() -> None:
    """Contract check against the real producer: clean scans parse, corrupted ones (id ending -X) don't."""
    events = build_live_events(history_days=7, end_day=date(2026, 10, 27), seed=42)
    for payload in events:
        if payload["event_id"].endswith("-X") and (payload["quantity"] <= 0 or payload["plant"] is None):
            with pytest.raises(InvalidEventError):
                parse_event(payload)
        elif not payload["event_id"].endswith("-X"):
            parse_event(payload)  # must not raise
