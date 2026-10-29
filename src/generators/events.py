"""Builds individual plant events in the exact schema the pipeline consumes.

Why a separate module:
    The event schema is a contract shared by the generator, the Lambda, Firehose and the Glue
    job. Defining it once here (field names, event types, reason codes, time format) avoids
    small differences between producers that would break the consumers.

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

# Event types: what happened at the scanner
CONSUMED = "CONSUMED"  # a station installed parts
REJECTED = "REJECTED"  # parts failed inspection and were scrapped
RECEIVED = "RECEIVED"  # the warehouse received a delivery
PRODUCED = "PRODUCED"  # finished units left the end of the line
EVENT_TYPES: tuple[str, ...] = (CONSUMED, REJECTED, RECEIVED, PRODUCED)

# Why a part was rejected: decides who pays for it
SUPPLIER_DEFECT = "SUPPLIER_DEFECT"  # arrived bad: recoverable from the supplier
PROCESS_DEFECT = "PROCESS_DEFECT"  # damaged by our station: internal loss
REASON_CODES: tuple[str, ...] = (SUPPLIER_DEFECT, PROCESS_DEFECT)

# Pseudo-stations for events that don't happen on an assembly station
WAREHOUSE = "WAREHOUSE"
END_OF_LINE = "END-OF-LINE"

# Both plants are in Mexico (UTC-6 all year since 2022). Fixed offset: no daylight-saving surprises.
PLANT_TZ = timezone(timedelta(hours=-6))


def to_utc_iso(local_time: datetime) -> str:
    """Convert a plant-local time to the UTC string every event carries.

    Why UTC: scanners in different time zones must sort and partition consistently. Plant-local
    concepts (shifts) are derived later from UTC with the plant's offset.

    Args:
        local_time: Timezone-aware datetime in plant local time.

    Returns:
        UTC time as "YYYY-MM-DDTHH:MM:SSZ".
    """
    return local_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
