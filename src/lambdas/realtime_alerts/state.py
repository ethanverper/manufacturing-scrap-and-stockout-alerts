"""All DynamoDB access of the real-time Lambda: idempotent updates, reads, and alert de-duplication.

Where it sits in the pipeline:
    After parsing.py validates an event, StateStore.apply_event() records it. The Lambda then reads
    the part's stock and the station's hourly counters and hands them to rules.py.

The three tables (created by Terraform; names arrive as environment variables):
    events_seen     key: dedupe_key                 one row per processed event_id, and per alert
                                                    sent (prefix "ALERT#"); TTL cleans them up
    part_stock      key: plant_part                 live units on hand per plant and part, seeded
                                                    with the end-of-history stock
    station_hourly  key: plant_station + hour       consumed / rejected counters per station per
                                                    UTC hour (the reject-rate window)

Why a single transaction (docs/decisions.md, ADR-003):
    Marking the event as seen, changing the stock and adding to the hourly counters must happen
    all together or not at all:
      - a re-sent scan (same event_id) cancels the whole transaction, so nothing is counted twice;
      - if the Lambda crashes halfway, nothing was written, so the retry is safe.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from src.common import config
from src.common.schema import CONSUMED
from src.lambdas.realtime_alerts.parsing import InvalidEventError, PlantEvent

# Outcomes of apply_event
APPLIED = "APPLIED"  # first time we see this event: state updated
DUPLICATE = "DUPLICATE"  # a retry of an event already processed: nothing changed

# Environment variable names (set by Terraform from config/settings.yaml)
ENV_EVENTS_SEEN_TABLE = "EVENTS_SEEN_TABLE"
ENV_PART_STOCK_TABLE = "PART_STOCK_TABLE"
ENV_STATION_HOURLY_TABLE = "STATION_HOURLY_TABLE"
ENV_EVENTS_SEEN_TTL_DAYS = "EVENTS_SEEN_TTL_DAYS"

# Hourly counters are only needed for the current hour; keep them 3 days for troubleshooting
STATION_HOURLY_TTL_SECONDS = 3 * 24 * 3600

# Key prefix that separates alert de-duplication rows from event rows in events_seen
ALERT_KEY_PREFIX = "ALERT#"

# DynamoDB's code for "the condition you set was false"
CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed"


class UnknownPartError(InvalidEventError):
    """The event names a plant/part with no row in part_stock (master data doesn't know it)."""


@dataclass(frozen=True)
class TableNames:
    """Physical names of the three tables (they include the project prefix Terraform adds)."""

    events_seen: str
    part_stock: str
    station_hourly: str

    @classmethod
    def from_env(cls) -> TableNames:
        """Read the table names from the Lambda's environment variables (required)."""
        return cls(
            events_seen=config.get_str(ENV_EVENTS_SEEN_TABLE),
            part_stock=config.get_str(ENV_PART_STOCK_TABLE),
            station_hourly=config.get_str(ENV_STATION_HOURLY_TABLE),
        )


class StateStore:
    """The Lambda's memory between invocations, kept in DynamoDB."""

    def __init__(
        self,
        client: Any,
        tables: TableNames,
        events_ttl_days: int,
        now: Callable[[], float] = time.time,
    ) -> None:
        """Wrap a DynamoDB client.

        Args:
            client: The client of a boto3 DynamoDB *resource* (`resource.meta.client`), which accepts
                plain Python values. Tests pass one backed by moto.
            tables: Table names.
            events_ttl_days: How long processed event ids are remembered (the retry window).
            now: Clock returning epoch seconds. Injectable so tests control TTL values.
        """
        self.client = client
        self.tables = tables
        self.events_ttl_seconds = events_ttl_days * 24 * 3600
        self.now = now

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def apply_event(self, event: PlantEvent) -> str:
        """Record an event exactly once: mark it seen, move stock, and count it for its station.

        Args:
            event: A validated event.

        Returns:
            APPLIED, or DUPLICATE if this event_id was already processed.

        Raises:
            UnknownPartError: The plant/part doesn't exist in part_stock. Bad data: don't retry.
        """
        items = self._transaction_items(event)
        try:
            self.client.transact_write_items(TransactItems=items)
        except self.client.exceptions.TransactionCanceledException as error:
            # DynamoDB says which item's condition failed, in the same order as `items`
            reasons = [reason.get("Code") for reason in error.response.get("CancellationReasons", [])]

            # Item 0 is the events_seen marker: its condition fails when the event was seen before
            if reasons and reasons[0] == CONDITIONAL_CHECK_FAILED:
                return DUPLICATE

            # Item 1 (when present) is the stock update: its condition fails for an unknown part
            if len(reasons) > 1 and reasons[1] == CONDITIONAL_CHECK_FAILED:
                raise UnknownPartError(f"Unknown plant/part: {event.plant_part}") from error

            # Anything else (throttling, conflicts) is transient: let the Lambda retry the record
            raise
        return APPLIED

    def claim_alert(self, dedupe_key: str) -> bool:
        """Reserve the right to send an alert. Only the first caller for a key gets True.

        Why: the same problem (a station's bad hour, a part's low day) keeps matching the rule on
        every new event. This conditional write makes sure only one email goes out per key, even
        if two Lambda invocations race.

        Args:
            dedupe_key: The alert's key, e.g. "LOW_STOCK#PLANT-MTY#CONNECTOR-C4#2026-10-28".

        Returns:
            True if this call reserved the key (send the alert), False if it was already sent.
        """
        try:
            self.client.put_item(
                TableName=self.tables.events_seen,
                Item={"dedupe_key": ALERT_KEY_PREFIX + dedupe_key, "expires_at": self._expires_at()},
                ConditionExpression="attribute_not_exists(dedupe_key)",
            )
        except self.client.exceptions.ConditionalCheckFailedException:
            # Someone already sent this alert
            return False
        return True

    # ------------------------------------------------------------------
    # Reads (strongly consistent: they must see the write we just made)
    # ------------------------------------------------------------------

    def part_stock(self, plant_part: str) -> dict[str, Any]:
        """Return on_hand, planned_daily_usage and supplier for a plant/part.

        Raises:
            UnknownPartError: If the row doesn't exist.
        """
        response = self.client.get_item(
            TableName=self.tables.part_stock, Key={"plant_part": plant_part}, ConsistentRead=True
        )
        item = response.get("Item")
        if item is None:
            raise UnknownPartError(f"Unknown plant/part: {plant_part}")

        # DynamoDB returns numbers as Decimal: convert to float for the KPI math
        return {
            "on_hand": float(item["on_hand"]),
            "planned_daily_usage": float(item["planned_daily_usage"]),
            "supplier": item["supplier"],
        }

    def station_hour(self, plant_station: str, hour: str) -> tuple[int, int]:
        """Return (consumed, rejected) units for a station in one UTC hour (0, 0 if nothing yet)."""
        response = self.client.get_item(
            TableName=self.tables.station_hourly,
            Key={"plant_station": plant_station, "hour": hour},
            ConsistentRead=True,
        )
        item = response.get("Item", {})
        return int(item.get("consumed", 0)), int(item.get("rejected", 0))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _expires_at(self, ttl_seconds: int | None = None) -> int:
        """Epoch second when a row should expire (DynamoDB TTL deletes it soon after, for free)."""
        return int(self.now()) + (ttl_seconds if ttl_seconds is not None else self.events_ttl_seconds)

    def _transaction_items(self, event: PlantEvent) -> list[dict[str, Any]]:
        """Build the all-or-nothing write for one event.

        Item order matters: apply_event reads the cancellation reasons by position
        (0 = seen marker, 1 = stock update).
        """
        # 0. The idempotency marker: fails if this event_id was already processed
        items: list[dict[str, Any]] = [
            {
                "Put": {
                    "TableName": self.tables.events_seen,
                    "Item": {"dedupe_key": event.event_id, "expires_at": self._expires_at()},
                    "ConditionExpression": "attribute_not_exists(dedupe_key)",
                }
            }
        ]

        # 1. The stock movement. ADD makes the update order-independent (ADR-005): late events
        #    can't corrupt the total. The condition rejects parts master data doesn't know.
        if event.part is not None and event.stock_delta != 0:
            items.append(
                {
                    "Update": {
                        "TableName": self.tables.part_stock,
                        "Key": {"plant_part": event.plant_part},
                        "UpdateExpression": "ADD on_hand :delta",
                        "ConditionExpression": "attribute_exists(plant_part)",
                        "ExpressionAttributeValues": {":delta": Decimal(event.stock_delta)},
                    }
                }
            )

        # 2. The station's hourly counter (installs and rejects only)
        if event.counts_for_reject_rate:
            counter = "consumed" if event.event_type == CONSUMED else "rejected"
            items.append(
                {
                    "Update": {
                        "TableName": self.tables.station_hourly,
                        "Key": {"plant_station": event.plant_station, "hour": event.hour_bucket},
                        # "consumed" is a DynamoDB reserved word, so the attribute goes through the
                        # #counter placeholder (caught by the moto tests before reaching AWS)
                        "UpdateExpression": (
                            "ADD #counter :quantity SET expires_at = if_not_exists(expires_at, :expires)"
                        ),
                        "ExpressionAttributeNames": {"#counter": counter},
                        "ExpressionAttributeValues": {
                            ":quantity": Decimal(event.quantity),
                            ":expires": self._expires_at(STATION_HOURLY_TTL_SECONDS),
                        },
                    }
                }
            )
        return items
