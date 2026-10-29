"""Load the opening stock into the part_stock DynamoDB table, before the live session starts.

Where it sits in the pipeline:
    src/generators/history.py writes data/generated/seed/stock_snapshot.csv: the stock of every
    plant and part at the end of the 7-day history. This script copies it into part_stock, so the
    real-time Lambda starts from exactly where the history ended, and the live session's numbers
    (for example, CONNECTOR-C4 starting at ~1.05 days of coverage) match the batch view.

Usage:
    python -m scripts.seed_stock --table <part_stock table name>
    (make seed runs it with the name from Terraform's outputs)
"""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.aws import get_resource
from src.common.logging_utils import get_logger

logger = get_logger(__name__)

DEFAULT_SNAPSHOT = Path("data/generated/seed/stock_snapshot.csv")


def snapshot_to_items(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Convert snapshot CSV rows into part_stock items.

    Why Decimal: DynamoDB stores numbers exactly, and boto3 refuses Python floats to avoid silent
    rounding. CSV values arrive as text, so they are converted straight to Decimal.

    Args:
        rows: Rows from stock_snapshot.csv.

    Returns:
        One item per plant and part, keyed by "PLANT#PART".
    """
    return [
        {
            "plant_part": f"{row['plant']}#{row['part']}",
            "plant": row["plant"],
            "part": row["part"],
            "supplier": row["supplier"],
            "on_hand": Decimal(row["on_hand"]),
            "planned_daily_usage": Decimal(row["planned_daily_usage"]),
        }
        for row in rows
    ]


def write_items(table: Any, items: list[dict[str, Any]]) -> None:
    """Write the items with a batch writer (groups puts into requests of 25 and retries throttled ones)."""
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Seed part_stock with the end-of-history stock.")
    parser.add_argument("--table", required=True, help="part_stock table name (terraform output)")
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT, help="stock_snapshot.csv path")
    args = parser.parse_args(argv)

    # Read the snapshot produced by `make generate`
    with args.snapshot.open(encoding="utf-8") as handle:
        items = snapshot_to_items(list(csv.DictReader(handle)))

    # Overwrite every row: re-seeding resets the stock to the end of the history
    write_items(get_resource("dynamodb").Table(args.table), items)
    logger.info("Stock seeded", extra={"fields": {"table": args.table, "items": len(items)}})


if __name__ == "__main__":
    main()
