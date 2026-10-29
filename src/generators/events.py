"""Builds individual plant events in the exact schema the pipeline consumes.

Why a separate module:
    The generator needs one place that turns simulated scans into events that follow the
    contract in src/common/schema.py exactly (same field names, types and time format), so the
    Lambda and the Glue job read demo data the same way they would read real scanners.

Schema (one JSON object per scan):
    event_id     unique per business event; retries re-send the same id
    event_time   UTC, ISO-8601 with "Z" (e.g. "2026-10-28T16:15:42Z")
    event_type   CONSUMED | REJECTED | RECEIVED | PRODUCED
    plant, line, station, part, supplier, lot
    quantity     units moved by this scan (always > 0 in a valid event)
    reason_code  only for REJECTED: SUPPLIER_DEFECT | PROCESS_DEFECT
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

# The contract (event types, reason codes, pseudo-stations, time format) lives in
# src/common/schema.py, shared with the Lambda and the Glue job. Re-exported here so the
# generator modules can keep importing everything event-related from one place.
from src.common.schema import (
    CONSUMED,
    END_OF_LINE,
    EVENT_TIME_FORMAT,
    EVENT_TYPES,
    PROCESS_DEFECT,
    PRODUCED,
    REASON_CODES,
    RECEIVED,
    REJECTED,
    SUPPLIER_DEFECT,
    WAREHOUSE,
)
from src.common.settings import settings

__all__ = [
    "CONSUMED",
    "END_OF_LINE",
    "EVENT_TYPES",
    "PLANT_TZ",
    "PROCESS_DEFECT",
    "PRODUCED",
    "REASON_CODES",
    "RECEIVED",
    "REJECTED",
    "SUPPLIER_DEFECT",
    "WAREHOUSE",
    "make_event",
    "make_event_id",
    "to_utc_iso",
]

# Plant local time zone (config/settings.yaml -> project.plant_utc_offset_hours).
# A fixed offset: the plants are in Mexico, UTC-6 all year, so there are no daylight-saving surprises.
PLANT_TZ = timezone(timedelta(hours=settings()["project"]["plant_utc_offset_hours"]))


def to_utc_iso(local_time: datetime) -> str:
    """Convert a plant-local time to the UTC string every event carries.

    Why UTC: scanners in different time zones must sort and partition consistently. Plant-local
    concepts (shifts) are derived later from UTC with the plant's offset.

    Args:
        local_time: Timezone-aware datetime in plant local time.

    Returns:
        UTC time as "YYYY-MM-DDTHH:MM:SSZ".
    """
    return local_time.astimezone(timezone.utc).strftime(EVENT_TIME_FORMAT)


def make_event_id(local_day: date, plant: str, sequence: int) -> str:
    """Build a unique, readable event id.

    Args:
        local_day: Plant-local date of the event.
        plant: Plant code; its last 3 letters keep the id short ("MTY").
        sequence: Global counter of the simulator, so ids never repeat across days.

    Returns:
        E.g. "EV-20261028-MTY-0001234".
    """
    return f"EV-{local_day:%Y%m%d}-{plant[-3:]}-{sequence:07d}"


def make_event(
    *,
    event_id: str,
    local_time: datetime,
    event_type: str,
    plant: str,
    quantity: int,
    line: str | None = None,
    station: str | None = None,
    part: str | None = None,
    supplier: str | None = None,
    lot: str | None = None,
    reason_code: str | None = None,
) -> dict:
    """Create one event dictionary in the pipeline schema.

    Keyword-only arguments make every call self-describing (no mixing up line and station).
    Fields that don't apply to an event type are None (JSON null), never missing, so every
    consumer sees the same set of keys.

    Returns:
        The event as a dict, ready to be serialized to JSON.
    """
    return {
        "event_id": event_id,
        "event_time": to_utc_iso(local_time),
        "event_type": event_type,
        "plant": plant,
        "line": line,
        "station": station,
        "part": part,
        "supplier": supplier,
        "lot": lot,
        "quantity": quantity,
        "reason_code": reason_code,
    }
