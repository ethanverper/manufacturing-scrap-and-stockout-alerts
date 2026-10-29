"""Stream the live session to Kinesis: the plant's scanners, the morning after the history.

Where it sits in the pipeline:
    The simulator replays the same 7 days as history.py (same seed, in memory, nothing written),
    so stock and lots are exactly where the history ended. It then simulates 06:00-09:00 local of
    the next day and sends those events to Kinesis Data Streams, paced to last a few minutes.

    During this session the planted incidents show up in real time: Voltex lot LOT-7781 pushes a
    station's hourly reject rate above 3%, and CONNECTOR-C4 coverage falls below 1 day.

Usage:
    python -m src.generators.live --stream <stream-name> --minutes 10
    python -m src.generators.live --dry-run --out data/generated/live_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import time
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from src.common.aws import get_client
from src.common.logging_utils import get_logger
from src.common.settings import settings
from src.generators.history import default_end_day
from src.generators.simulator import PlantSimulator, add_scanner_noise

logger = get_logger(__name__)

# Simulation settings from config/settings.yaml (section "simulation")
_SIMULATION = settings()["simulation"]
_SESSION = _SIMULATION["live_session"]

# The live session covers the start of the morning shift, plant local time
LIVE_HOURS = range(_SESSION["start_hour"], _SESSION["end_hour"])

# Live noise: more retries than the history, and a few corrupted scans for the Lambda's validation
LIVE_DUPLICATE_RATE: float = _SIMULATION["noise"]["live"]["duplicate_rate"]
LIVE_INVALID_RATE: float = _SIMULATION["noise"]["live"]["invalid_rate"]

# Protocol constants (not settings): Kinesis PutRecords accepts at most 500 records and 5 MB per call
MAX_RECORDS_PER_CALL: int = 500
MAX_BYTES_PER_CALL: int = 4_500_000

# Retries for records Kinesis throttles (ProvisionedThroughputExceededException)
MAX_ATTEMPTS: int = 6


def build_live_events(history_days: int, end_day: date, seed: int) -> list[dict]:
    """Recreate the end-of-history plant state and simulate the live morning.

    Args:
        history_days: Same value used by history.py.
        end_day: Same last history day used by history.py.
        seed: Same seed used by history.py.

    Returns:
        Live events in time order, with scanner noise.
    """
    # Replay the history in memory, discarding the events: we only need the final plant state
    first_day = end_day - timedelta(days=history_days - 1)
    simulator = PlantSimulator(history_days=history_days, first_day=first_day, seed=seed)
    for day in range(history_days):
        simulator.simulate_day(day)

    # The live session is the day right after the history, morning hours only
    clean_events = simulator.simulate_day(history_days, hours=LIVE_HOURS)

    # A different noise seed from history, so the live retries are not copies of the history ones
    return add_scanner_noise(clean_events, random.Random(seed + 2), LIVE_DUPLICATE_RATE, LIVE_INVALID_RATE)


def to_kinesis_record(event: dict) -> dict[str, Any]:
    """Wrap an event as a Kinesis record.

    - Data: the JSON plus a newline. Firehose concatenates records as-is, so the newline is what
      keeps one event per line in S3.
    - PartitionKey: "plant#part", so every movement of a part in a plant goes to the same shard,
      in order. Events without a part (PRODUCED) use "plant#line".
    """
    key_detail = event.get("part") or event.get("line") or "NONE"
    return {
        "Data": (json.dumps(event) + "\n").encode("utf-8"),
        "PartitionKey": f"{event.get('plant')}#{key_detail}",
    }


def batches(
    records: list[dict], max_count: int = MAX_RECORDS_PER_CALL, max_bytes: int = MAX_BYTES_PER_CALL
) -> Iterator[list[dict]]:
    """Split records into groups that respect the PutRecords limits.

    Args:
        records: Kinesis records (dicts with Data and PartitionKey).
        max_count: Maximum records per call.
        max_bytes: Maximum payload bytes per call.

    Yields:
        Lists of records, in their original order.
    """
    batch: list[dict] = []
    size = 0
    for record in records:
        record_size = len(record["Data"]) + len(record["PartitionKey"])

        # Close the current batch when one more record would break either limit
        if batch and (len(batch) == max_count or size + record_size > max_bytes):
            yield batch
            batch, size = [], 0

        batch.append(record)
        size += record_size

    # Whatever is left forms the last batch
    if batch:
        yield batch


def put_with_backoff(client: Any, stream: str, records: list[dict], max_attempts: int = MAX_ATTEMPTS) -> int:
    """Send records with PutRecords, retrying only the ones Kinesis rejected.

    Why: PutRecords is not all-or-nothing. Some records can fail (throttling) while others
    succeed. Re-sending the whole batch would duplicate the successful ones.

    Args:
        client: A boto3 Kinesis client (or a fake in tests).
        stream: Stream name.
        records: Records to send.
        max_attempts: How many times to try before giving up.

    Returns:
        The number of records that still failed after every attempt (0 means all were delivered).
    """
    pending = records
    for attempt in range(max_attempts):
        response = client.put_records(StreamName=stream, Records=pending)

        # Everything was accepted: done
        if response["FailedRecordCount"] == 0:
            return 0

        # Keep only the failed records. The response lists results in the same order as the request.
        pending = [record for record, result in zip(pending, response["Records"], strict=True) if "ErrorCode" in result]

        # Exponential backoff with jitter: 0.1 s, 0.2 s, 0.4 s, ... capped at 10 s
        time.sleep(min(10.0, 0.1 * 2**attempt) * random.uniform(0.5, 1.5))
    return len(pending)


def stream_events(events: list[dict], stream: str, minutes: float, client: Any) -> int:
    """Send the events to Kinesis, spread evenly over `minutes` of wall-clock time.

    Pacing makes the demo watchable: the alerts arrive while the session is still running,
    instead of everything landing in one second.

    Returns:
        Records that could not be delivered.
    """
    records = [to_kinesis_record(event) for event in events]

    # Small batches (one second's worth of events) keep the flow steady instead of bursty
    records_per_second = max(1, round(len(records) / (minutes * 60)))
    failed = 0
    for batch in batches(records, max_count=records_per_second):
        started = time.monotonic()
        failed += put_with_backoff(client, stream, batch)

        # Sleep for the rest of the second, if the call was faster than that
        time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))
    return failed


def write_jsonl(events: list[dict], path: Path) -> None:
    """Dry run: write the events to a local JSON-lines file instead of Kinesis."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point: build the live session and stream it (or write it locally)."""
    parser = argparse.ArgumentParser(description="Stream the live plant session to Kinesis.")
    parser.add_argument("--stream", help="Kinesis stream name (required unless --dry-run)")
    parser.add_argument(
        "--minutes", type=float, default=_SESSION["wall_clock_minutes"], help="wall-clock duration of the session"
    )
    parser.add_argument(
        "--days", type=int, default=_SIMULATION["history_days"], help="history days (must match history.py)"
    )
    parser.add_argument("--end-day", type=date.fromisoformat, default=None, help="last history day (default yesterday)")
    parser.add_argument("--seed", type=int, default=_SIMULATION["seed"], help="random seed (must match history.py)")
    parser.add_argument("--dry-run", action="store_true", help="write events to --out instead of Kinesis")
    parser.add_argument("--out", type=Path, default=Path("data/generated/live_events.jsonl"))
    args = parser.parse_args(argv)

    # Same days, end day and seed as the history -> the same plant state
    events = build_live_events(args.days, args.end_day or default_end_day(), args.seed)

    # Dry run: inspect the session locally, no AWS needed
    if args.dry_run:
        write_jsonl(events, args.out)
        logger.info("Live session written locally", extra={"fields": {"events": len(events), "out": str(args.out)}})
        return

    # A stream name is mandatory for the real thing
    if not args.stream:
        parser.error("--stream is required unless --dry-run is set")

    failed = stream_events(events, args.stream, args.minutes, get_client("kinesis"))
    logger.info(
        "Live session streamed", extra={"fields": {"events": len(events), "failed": failed, "stream": args.stream}}
    )

    # Exit with an error if anything was lost, so `make` notices
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
