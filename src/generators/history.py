"""Write the 7-day event history, the master data and the opening stock to a local folder.

Where it sits in the pipeline:
    The output folder mirrors the S3 bucket layout, so a single `aws s3 sync` uploads it:

        <out>/raw/plant_events/dt=YYYY-MM-DD/history.json.gz   (as if Firehose had delivered it)
        <out>/reference/parts/parts.csv                         (cost, supplier, planned usage)
        <out>/reference/plan/plan.csv                           (planned units per line and hour)
        <out>/reference/stations/stations.csv                   (plant -> line -> station)
        <out>/seed/stock_snapshot.csv                           (stock at the end of the history)

    The stock snapshot seeds the real-time state in DynamoDB, so the live session continues
    exactly where the history left off.

Usage:
    python -m src.generators.history --days 7 --out data/generated
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from src.common.logging_utils import get_logger
from src.generators import reference
from src.generators.events import PLANT_TZ
from src.generators.simulator import PlantSimulator, add_scanner_noise

logger = get_logger(__name__)

# Scanner noise in the history: 1% retries (duplicates), 0.1% corrupted scans
HISTORY_DUPLICATE_RATE: float = 0.01
HISTORY_INVALID_RATE: float = 0.001


def default_end_day() -> date:
    """Yesterday in plant local time: the history ends the day before the live session."""
    return datetime.now(PLANT_TZ).date() - timedelta(days=1)


def simulate_history(days: int, end_day: date, seed: int) -> tuple[PlantSimulator, list[dict]]:
    """Run the plants for `days` full days ending on `end_day`.

    Args:
        days: Number of history days.
        end_day: Last history day (plant local date).
        seed: Random seed shared with live.py, so both reach the same plant state.

    Returns:
        (the simulator, positioned at the end of the history; the events, with scanner noise).
    """
    # Day 0 is the first history day; the live session will be day `days` (the day after end_day)
    first_day = end_day - timedelta(days=days - 1)
    simulator = PlantSimulator(history_days=days, first_day=first_day, seed=seed)

    # Simulate every hour of every history day
    clean_events: list[dict] = []
    for day in range(days):
        clean_events.extend(simulator.simulate_day(day))

    # Add retries and corrupted scans with a separate generator (keeps the plant state intact)
    noise_rng = random.Random(seed + 1)
    events = add_scanner_noise(clean_events, noise_rng, HISTORY_DUPLICATE_RATE, HISTORY_INVALID_RATE)
    return simulator, events


def write_raw_events(events: list[dict], out_dir: Path) -> dict[str, int]:
    """Write events as gzipped JSON lines, one folder per UTC date (`dt=YYYY-MM-DD`).

    Why this layout: it is exactly what Firehose produces (Hive-style `dt=` folders, one JSON
    object per line, GZIP), so Athena and the Glue job read history and live data the same way.

    Args:
        events: Events to write.
        out_dir: Root output folder.

    Returns:
        Number of events written per date.
    """
    # Group by the UTC date of the event: the first 10 characters of event_time
    by_date: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        by_date[event["event_time"][:10]].append(event)

    # One gzipped JSON-lines file per date folder
    for dt, day_events in sorted(by_date.items()):
        folder = out_dir / "raw" / "plant_events" / f"dt={dt}"
        folder.mkdir(parents=True, exist_ok=True)
        with gzip.open(folder / "history.json.gz", "wt", encoding="utf-8") as handle:
            for event in day_events:
                handle.write(json.dumps(event) + "\n")

    return {dt: len(day_events) for dt, day_events in sorted(by_date.items())}


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write rows to a CSV file with a header, creating the folder if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_reference_data(out_dir: Path, plan_days: list[date]) -> None:
    """Export the master data. Each table gets its own folder, so Athena can map one table to it.

    Args:
        out_dir: Root output folder.
        plan_days: Days to include in the production plan (history plus the live day).
    """
    write_csv(out_dir / "reference" / "parts" / "parts.csv", reference.parts_rows())
    write_csv(out_dir / "reference" / "plan" / "plan.csv", reference.plan_rows(plan_days))
    write_csv(out_dir / "reference" / "stations" / "stations.csv", reference.stations_rows())


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point: simulate the history and write everything to `--out`."""
    # Command-line options, with defaults that match the spec
    parser = argparse.ArgumentParser(description="Write the plant event history and master data.")
    parser.add_argument("--days", type=int, default=7, help="history days (default 7)")
    parser.add_argument("--end-day", type=date.fromisoformat, default=None, help="last history day (default yesterday)")
    parser.add_argument("--seed", type=int, default=42, help="random seed (must match live.py)")
    parser.add_argument("--out", type=Path, default=Path("data/generated"), help="output folder")
    args = parser.parse_args(argv)

    # Simulate the history and keep the simulator: its final stock seeds the live state
    end_day = args.end_day or default_end_day()
    simulator, events = simulate_history(args.days, end_day, args.seed)

    # The plan covers every history day plus the live-session day
    plan_days = [simulator.calendar_day(day) for day in range(args.days + 1)]

    # Write the three outputs
    counts = write_raw_events(events, args.out)
    write_reference_data(args.out, plan_days)
    write_csv(args.out / "seed" / "stock_snapshot.csv", simulator.stock_snapshot_rows())

    logger.info(
        "History written",
        extra={"fields": {"events": len(events), "events_per_day": counts, "out": str(args.out)}},
    )


if __name__ == "__main__":
    main()
