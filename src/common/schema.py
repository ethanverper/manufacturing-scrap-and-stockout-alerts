"""The plant event contract: field names, event types and reason codes.

Why it lives in src/common/:
    Every component speaks this schema: the producers (scanners, or src/generators/ in the demo),
    the real-time Lambda (src/lambdas/realtime_alerts/), Firehose and the Glue job. Defining it once,
    in a module none of them owns, keeps them in agreement and lets the Lambda be deployed without
    the generator.

These are protocol constants, not settings: changing them breaks every consumer, so they are
deliberately kept in code and not in config/settings.yaml.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Event types: what happened at the scanner
# ---------------------------------------------------------------------------
CONSUMED = "CONSUMED"  # a station installed parts
REJECTED = "REJECTED"  # parts failed inspection and were scrapped
RECEIVED = "RECEIVED"  # the warehouse received a delivery
PRODUCED = "PRODUCED"  # finished units left the end of the line
EVENT_TYPES: tuple[str, ...] = (CONSUMED, REJECTED, RECEIVED, PRODUCED)

# ---------------------------------------------------------------------------
# Reason codes: why a part was rejected, which decides who pays for it
# ---------------------------------------------------------------------------
SUPPLIER_DEFECT = "SUPPLIER_DEFECT"  # arrived bad: recoverable from the supplier
PROCESS_DEFECT = "PROCESS_DEFECT"  # damaged by our station: internal loss
REASON_CODES: tuple[str, ...] = (SUPPLIER_DEFECT, PROCESS_DEFECT)

# ---------------------------------------------------------------------------
# Pseudo-stations for events that don't happen on an assembly station
# ---------------------------------------------------------------------------
WAREHOUSE = "WAREHOUSE"
END_OF_LINE = "END-OF-LINE"

# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------

# Every event carries all of these keys (null when they don't apply)
ALL_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "event_type",
    "plant",
    "line",
    "station",
    "part",
    "supplier",
    "lot",
    "quantity",
    "reason_code",
)

# Fields that must have a value, by event type (the rest may be null)
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    CONSUMED: ("event_id", "event_time", "plant", "line", "station", "part", "supplier", "lot", "quantity"),
    REJECTED: (
        "event_id",
        "event_time",
        "plant",
        "line",
        "station",
        "part",
        "supplier",
        "lot",
        "quantity",
        "reason_code",
    ),
    RECEIVED: ("event_id", "event_time", "plant", "station", "part", "supplier", "lot", "quantity"),
    PRODUCED: ("event_id", "event_time", "plant", "line", "station", "quantity"),
}

# Timestamp format of event_time: UTC with a trailing "Z"
EVENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
