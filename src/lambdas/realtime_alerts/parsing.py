"""Turn a raw Kinesis record into a validated, typed plant event.

Where it sits in the pipeline:
    First step of the real-time Lambda. Kinesis delivers each scan as base64-encoded JSON. This
    module decodes it and checks it against the contract in src/common/schema.py. Only a valid
    `PlantEvent` moves on to the business rules and DynamoDB.

Why validate here:
    Scanners send corrupted records (negative quantity, missing plant, malformed JSON). One bad
    record must not crash the whole batch or corrupt the stock. It is rejected with a clear reason
    (InvalidEventError), logged, and skipped. The raw copy is still kept in S3 by Firehose, and the
    Glue job quarantines it there.

What is NOT checked here:
    Whether the part or the station exists. That needs master data, which lives in DynamoDB (the
    seeded part_stock table), so that check happens in the state layer (level 3).
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.schema import (
    ALL_FIELDS,
    CONSUMED,
    EVENT_TIME_FORMAT,
    EVENT_TYPES,
    PRODUCED,
    REASON_CODES,
    RECEIVED,
    REJECTED,
    REQUIRED_FIELDS,
)


class InvalidEventError(ValueError):
    """Raised when a record breaks the event contract. The message says exactly what is wrong."""


@dataclass(frozen=True)
class PlantEvent:
    """A validated plant event. Frozen (read-only), so no later step can alter what was scanned."""

    event_id: str
    event_time: datetime  # UTC, timezone-aware
    event_type: str
    plant: str
    line: str | None
    station: str | None
    part: str | None
    supplier: str | None
    lot: str | None
    quantity: int
    reason_code: str | None

    @property
    def stock_delta(self) -> int:
        """How this event changes the units on the shelf.

        Deliveries add units. Installs and rejects remove them: a rejected part was taken from the
        shelf and scrapped. Finished units don't touch part stock.
        """
        if self.event_type == RECEIVED:
            return self.quantity
        if self.event_type == PRODUCED:
            return 0
        return -self.quantity  # CONSUMED or REJECTED

    @property
    def hour_bucket(self) -> str:
        """UTC clock hour of the event, e.g. "2026-10-28T13": the window of the reject-rate alert."""
        return self.event_time.strftime("%Y-%m-%dT%H")

    @property
    def plant_part(self) -> str:
        """DynamoDB key of the part's stock in this plant, e.g. "PLANT-MTY#BATTERY-A12"."""
        return f"{self.plant}#{self.part}"

    @property
    def plant_station(self) -> str:
        """DynamoDB key of the station's hourly counters, e.g. "PLANT-MTY#LINE-2-ASSEMBLY-3"."""
        return f"{self.plant}#{self.station}"

    @property
    def counts_for_reject_rate(self) -> bool:
        """True for events at an assembly station that feed the reject rate (installs and rejects)."""
        return self.event_type in (CONSUMED, REJECTED)


def decode_kinesis_record(record: dict) -> dict:
    """Extract the JSON payload from one record of a Kinesis-triggered Lambda event.

    Args:
        record: One element of event["Records"], as Lambda delivers it.

    Returns:
        The decoded JSON object.

    Raises:
        InvalidEventError: If the data is not valid base64, UTF-8, or a JSON object.
    """
    try:
        # Kinesis always base64-encodes the bytes the producer sent
        text = base64.b64decode(record["kinesis"]["data"], validate=True).decode("utf-8")

        # The producer adds a trailing newline for Firehose; json.loads ignores surrounding whitespace
        payload = json.loads(text)
    except (KeyError, binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidEventError(f"Undecodable record: {error}") from error

    # A valid scan is a JSON object, never a list or a bare value
    if not isinstance(payload, dict):
        raise InvalidEventError("Payload is not a JSON object")
    return payload


def parse_event(payload: dict) -> PlantEvent:
    """Validate a decoded payload against the contract and return a typed event.

    Checks, in order: known event type, required fields present for that type, quantity is a
    positive integer, reason code valid (rejects only), and event time in the contract's format.

    Args:
        payload: The decoded JSON object.

    Returns:
        A PlantEvent.

    Raises:
        InvalidEventError: On the first broken rule, with a message naming the field.
    """
    # 1. The event type decides which fields are required, so it is checked first
    event_type = payload.get("event_type")
    if event_type not in EVENT_TYPES:
        raise InvalidEventError(f"Unknown event_type: {event_type!r}")

    # 2. Every required field for this type must have a value (null or "" doesn't count)
    missing = [field for field in REQUIRED_FIELDS[event_type] if payload.get(field) in (None, "")]
    if missing:
        raise InvalidEventError(f"Missing required fields for {event_type}: {missing}")

    # 3. Quantity must be a positive whole number. bool is excluded on purpose: in Python, True == 1.
    quantity = payload["quantity"]
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise InvalidEventError(f"quantity must be a positive integer, got {quantity!r}")

    # 4. Rejects must say why; other events must not carry a reason
    reason_code = payload.get("reason_code")
    if event_type == REJECTED and reason_code not in REASON_CODES:
        raise InvalidEventError(f"Invalid reason_code for REJECTED: {reason_code!r}")
    if event_type != REJECTED and reason_code is not None:
        raise InvalidEventError(f"reason_code is only allowed on REJECTED events, got {reason_code!r}")

    # 5. The timestamp must follow the contract (UTC with "Z") so hour buckets are consistent
    try:
        event_time = datetime.strptime(payload["event_time"], EVENT_TIME_FORMAT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError) as error:
        raise InvalidEventError(f"event_time not in {EVENT_TIME_FORMAT}: {payload['event_time']!r}") from error

    # Build the typed event. Optional fields that are absent become None.
    values = {field: payload.get(field) for field in ALL_FIELDS}
    values.update(event_time=event_time, quantity=quantity)
    return PlantEvent(**values)
