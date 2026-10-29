"""Lambda entry point: Kinesis batch in, DynamoDB state and SNS alerts out.

Flow for every record in the batch:
    1. parsing.py   decode and validate                -> invalid: log and skip (never retried)
    2. state.py     idempotent transaction             -> duplicate: log and stop here
    3. state.py     read stock and station counters
    4. rules.py     decide whether to alert
    5. state.py     claim the alert (once per problem) -> notifier.py sends the email

Error handling (event source mapping with ReportBatchItemFailures):
    - Bad data (InvalidEventError) is skipped: retrying a corrupted scan can never fix it.
    - Anything else (throttling, a network error) is reported as a batch item failure, so Lambda
      retries from that record. Records that were already applied come back as DUPLICATE, so the
      retry is safe.

The handler itself holds no business logic; it only wires the modules together.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import Any

from src.common import config
from src.common.aws import get_client, get_resource
from src.common.logging_utils import get_logger
from src.lambdas.realtime_alerts import rules
from src.lambdas.realtime_alerts.notifier import Notifier
from src.lambdas.realtime_alerts.parsing import InvalidEventError, PlantEvent, decode_kinesis_record, parse_event
from src.lambdas.realtime_alerts.state import DUPLICATE, ENV_EVENTS_SEEN_TTL_DAYS, StateStore, TableNames

logger = get_logger(__name__)


@dataclass(frozen=True)
class Dependencies:
    """Everything the handler needs, created once per Lambda container and reused across invocations."""

    store: StateStore
    notifier: Notifier
    thresholds: rules.AlertThresholds


@cache
def _dependencies() -> Dependencies:
    """Build the AWS-backed dependencies from the environment (once per container).

    Tests replace this function with one that returns moto-backed or fake dependencies.
    """
    return Dependencies(
        store=StateStore(
            client=get_resource("dynamodb").meta.client,
            tables=TableNames.from_env(),
            events_ttl_days=config.get_int(ENV_EVENTS_SEEN_TTL_DAYS, 2),
        ),
        notifier=Notifier.from_env(get_client("sns")),
        thresholds=rules.AlertThresholds.from_env(),
    )


def evaluate_alerts(event: PlantEvent, store: StateStore, thresholds: rules.AlertThresholds) -> list[rules.Alert]:
    """Read the state this event just changed and return any alerts it triggers.

    Args:
        event: The event that was just applied.
        store: Where the stock and counters live.
        thresholds: Alert thresholds.

    Returns:
        Zero, one or two alerts (low stock and/or high reject rate).
    """
    alerts: list[rules.Alert] = []

    # Stock can only run low when parts leave the shelf (installs and rejects)
    if event.part is not None and event.stock_delta < 0:
        stock = store.part_stock(event.plant_part)
        low_stock = rules.check_low_stock(
            plant=event.plant,
            part=event.part,
            supplier=stock["supplier"],
            on_hand=stock["on_hand"],
            planned_daily_usage=stock["planned_daily_usage"],
            day=event.event_time.date().isoformat(),
            thresholds=thresholds,
        )
        if low_stock is not None:
            alerts.append(low_stock)

    # The station's reject rate only changes with installs and rejects
    if event.counts_for_reject_rate:
        consumed, rejected = store.station_hour(event.plant_station, event.hour_bucket)
        high_rejects = rules.check_reject_rate(
            plant=event.plant,
            station=event.station,
            hour_bucket=event.hour_bucket,
            consumed_units=consumed,
            rejected_units=rejected,
            thresholds=thresholds,
        )
        if high_rejects is not None:
            alerts.append(high_rejects)
    return alerts


def process_record(record: dict, deps: Dependencies) -> str:
    """Handle one Kinesis record end to end.

    Returns:
        "APPLIED", "DUPLICATE", or "ALERTED:<n>" when n alerts were sent (used in logs and tests).

    Raises:
        InvalidEventError: The record breaks the contract (the caller skips it).
        Exception: Any transient AWS error (the caller reports it for retry).
    """
    # 1. Validate: raises InvalidEventError for corrupted scans
    event = parse_event(decode_kinesis_record(record))

    # 2. Record it exactly once; a retry of an already-processed scan stops here
    if deps.store.apply_event(event) == DUPLICATE:
        logger.info("Duplicate event skipped", extra={"fields": {"event_id": event.event_id}})
        return DUPLICATE

    # 3-4. Check the rules against the updated state
    sent = 0
    for alert in evaluate_alerts(event, deps.store, deps.thresholds):
        # 5. Only the first event that matches a problem sends the email
        if deps.store.claim_alert(alert.dedupe_key):
            message_id = deps.notifier.publish(alert)
            sent += 1
            logger.warning(
                "Alert sent",
                extra={"fields": {"kind": alert.kind, "message_id": message_id, **alert.metrics}},
            )
    return f"ALERTED:{sent}" if sent else "APPLIED"


def handler(event: dict, context: Any) -> dict:
    """AWS Lambda entry point for the Kinesis event source mapping.

    Args:
        event: {"Records": [...]} with up to `batch_size` Kinesis records.
        context: Lambda context (unused).

    Returns:
        {"batchItemFailures": [...]}: the records Lambda must retry.
    """
    deps = _dependencies()
    failures: list[dict[str, str]] = []

    for record in event["Records"]:
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            process_record(record, deps)
        except InvalidEventError as error:
            # Bad data won't fix itself: log it and move on (Firehose keeps the raw copy in S3)
            logger.warning(
                "Invalid event skipped", extra={"fields": {"sequence_number": sequence_number, "reason": str(error)}}
            )
        except Exception as error:  # noqa: BLE001 (any other failure must be retried, never lost)
            # Transient problem: ask Lambda to retry from this record
            logger.error(
                "Record failed, will be retried",
                extra={"fields": {"sequence_number": sequence_number, "error": repr(error)}},
            )
            failures.append({"itemIdentifier": sequence_number})

    return {"batchItemFailures": failures}
