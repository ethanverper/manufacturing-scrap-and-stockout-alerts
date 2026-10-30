"""Glue job (PySpark): turn the raw plant events into clean events and one gold table per KPI.

Where it sits in the pipeline:
    Firehose (live) and history.py (backfill) put raw events in s3://<bucket>/raw/plant_events/.
    This job runs once a day (trigger disabled by default, ADR-009) or on demand (`make kpis`):

        extract()  raw events + reference CSVs
        transform  transforms.py: validate → deduplicate → enrich → build one table per KPI
        load()     curated/, gold/<kpi>/, quarantine/   (Parquet, partitioned by dt)

The same file runs in two places:
    - On AWS Glue 5.0 (Spark 3.5): Terraform uploads it and passes --input_root s3://<bucket>.
    - On a laptop: `make kpis-local` runs it on data/generated with a local Spark (no AWS).
    It uses plain Spark, not Glue's DynamicFrames: the data has a fixed schema and no job
    bookmarks are needed, so the code stays portable and testable.

Why a full refresh on every run (docs/decisions.md, ADR-011):
    The stock position is a running sum from the opening stock, so it needs every day anyway, and
    the whole lake is ~50,000 events. Writing with dynamic partition overwrite replaces only the
    dates present, so a re-run gives exactly the same tables (idempotent).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from src.common.logging_utils import get_logger
from src.glue_jobs import transforms

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Lake layout (docs/architecture.md, section 4). Protocol, not settings: every reader depends on it.
# ---------------------------------------------------------------------------
RAW_EVENTS = "raw/plant_events"
REFERENCE = "reference"
CURATED_EVENTS = "curated/events_clean"
QUARANTINE = "quarantine/plant_events"
GOLD = "gold"

# Reference table name -> its Spark schema (one folder per table under reference/)
REFERENCE_TABLES = {
    "parts": transforms.PARTS_SCHEMA,
    "stations": transforms.STATIONS_SCHEMA,
    "plan": transforms.PLAN_SCHEMA,
    "opening_stock": transforms.OPENING_STOCK_SCHEMA,
}

# Columns kept in the curated layer: the event contract plus the derived and enriched columns
CURATED_COLUMNS = [
    "event_id",
    "event_time",
    "event_ts",
    "event_type",
    "plant",
    "line",
    "station",
    "part",
    "supplier",
    "supplier_name",
    "lot",
    "quantity",
    "reason_code",
    "unit_cost",
    "stock_delta",
    "local_date",
    "local_hour",
    "shift",
    "dt",
]


@dataclass(frozen=True)
class JobArgs:
    """Everything the job needs to know. On Glue, Terraform passes each value as a job argument.

    Attributes:
        input_root: Where raw/ and reference/ are: "s3://<bucket>" or a local folder.
        output_root: Where curated/, gold/ and quarantine/ go (the same bucket on AWS).
        utc_offset_hours: Plant time minus UTC (config: project.plant_utc_offset_hours).
        shift_start_hours: Shift name -> start hour (config: plant.shift_start_hours).
        coverage_threshold_days: Low-stock threshold (config: alerts.stock_coverage_threshold_days).
    """

    input_root: str
    output_root: str
    utc_offset_hours: int
    shift_start_hours: dict[str, int]
    coverage_threshold_days: float


def parse_args(argv: list[str] | None = None) -> JobArgs:
    """Read the job arguments from the command line.

    Why parse_known_args: Glue adds its own arguments (--JOB_ID, --JOB_RUN_ID, --TempDir...) that
    this job doesn't use; they are ignored instead of failing the run.

    Args:
        argv: Arguments to parse (None = the real command line).

    Returns:
        The parsed arguments.
    """
    # The names match the default_arguments in infra/terraform/4_batch.tf
    parser = argparse.ArgumentParser(description="Build the daily KPI tables from raw plant events.")
    parser.add_argument("--input_root", required=True)
    parser.add_argument("--output_root", required=True)
    parser.add_argument("--utc_offset_hours", type=int, required=True)
    parser.add_argument("--shift_start_hours", type=json.loads, required=True, help='JSON, e.g. {"MORNING": 6}')
    parser.add_argument("--coverage_threshold_days", type=float, required=True)
    known, _glue_extras = parser.parse_known_args(argv)

    # Remove a trailing "/" so paths can always be joined with one
    return JobArgs(
        input_root=known.input_root.rstrip("/"),
        output_root=known.output_root.rstrip("/"),
        utc_offset_hours=known.utc_offset_hours,
        shift_start_hours={name: int(hour) for name, hour in known.shift_start_hours.items()},
        coverage_threshold_days=known.coverage_threshold_days,
    )


def build_spark(app_name: str = "daily-kpis") -> SparkSession:
    """Create (or reuse, on Glue) the Spark session with the two settings the job relies on.

    - Session time zone UTC: event_time is UTC, and dt must be the UTC date wherever it runs.
    - Dynamic partition overwrite: writing a table replaces only the dt partitions in the new data,
      never the whole table, which makes re-runs safe.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def extract(spark: SparkSession, input_root: str) -> tuple[DataFrame, dict[str, DataFrame]]:
    """Read the raw events and the reference tables.

    Args:
        spark: The Spark session.
        input_root: Folder or s3:// root that holds raw/ and reference/.

    Returns:
        (raw events, reference tables by name).
    """
    # Raw events: gzipped JSON lines in dt= folders. recursiveFileLookup reads every file and skips
    # the folder's dt value on purpose: Firehose names folders by ARRIVAL date, the job uses the
    # event's own date (an event scanned at 23:59:59 may arrive after midnight).
    raw = (
        spark.read.schema(transforms.RAW_EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(f"{input_root}/{RAW_EVENTS}/")
    )

    # Reference CSVs, each with its declared schema
    reference = {
        name: spark.read.option("header", "true").schema(schema).csv(f"{input_root}/{REFERENCE}/{name}/")
        for name, schema in REFERENCE_TABLES.items()
    }
    return raw, reference


def transform(raw: DataFrame, reference: dict[str, DataFrame], args: JobArgs) -> dict[str, DataFrame]:
    """Run every transformation of transforms.py in order.

    Args:
        raw: Raw events.
        reference: Reference tables by name.
        args: Job arguments (time zone, shifts, threshold).

    Returns:
        Output tables by their path under the output root (e.g. "gold/reject_rate").
    """
    # Time columns first: validation needs event_ts, and every KPI needs dt
    events = transforms.add_event_columns(raw, args.utc_offset_hours, args.shift_start_hours)

    # Clean: quarantine what breaks a rule, count each scan once, add master data
    valid, quarantine = transforms.validate(events, reference["parts"], reference["stations"])
    curated = transforms.enrich(transforms.deduplicate(valid), reference["parts"])

    # Reused by five tables: cache it so Spark doesn't recompute the cleaning for each one
    curated = curated.cache()

    # One gold table per KPI, plus the record reconciliation
    return {
        CURATED_EVENTS: curated.select(*CURATED_COLUMNS),
        QUARANTINE: quarantine,
        f"{GOLD}/reject_rate": transforms.build_reject_rate(curated),
        f"{GOLD}/scrap_cost": transforms.build_scrap_cost(curated),
        f"{GOLD}/throughput": transforms.build_throughput(
            curated, reference["plan"], args.utc_offset_hours, args.shift_start_hours
        ),
        f"{GOLD}/stock_coverage": transforms.build_stock_coverage(
            curated, reference["opening_stock"], reference["parts"], args.coverage_threshold_days
        ),
        f"{GOLD}/data_quality": transforms.build_data_quality(events, valid, curated, quarantine),
    }


def load(tables: dict[str, DataFrame], output_root: str) -> None:
    """Write every table as Parquet, partitioned by dt, replacing only the dates present.

    Args:
        tables: Output tables by path (from transform()).
        output_root: Folder or s3:// root to write under.
    """
    for path, frame in tables.items():
        frame.write.mode("overwrite").partitionBy("dt").parquet(f"{output_root}/{path}/")


def summarize(tables: dict[str, DataFrame]) -> dict[str, object]:
    """Small run summary for the logs: record reconciliation totals and rows per output table."""
    # The data-quality table is tiny (one row per day): bring it to the driver
    quality = tables[f"{GOLD}/data_quality"].collect()
    return {
        "raw_records": sum(row["raw_records"] for row in quality),
        "curated_records": sum(row["curated_records"] for row in quality),
        "duplicate_records": sum(row["duplicate_records"] for row in quality),
        "quarantined_records": sum(row["quarantined_records"] for row in quality),
        "all_days_balanced": all(row["is_balanced"] for row in quality),
        "rows_per_table": {path: frame.count() for path, frame in tables.items()},
    }


def run(spark: SparkSession, args: JobArgs) -> dict[str, object]:
    """Extract, transform and load. Returns the run summary (also logged).

    Raises:
        RuntimeError: If the record counts don't reconcile (some record was lost). Nothing is
            written: failing the job is better than publishing KPIs built on missing data.
    """
    # Extract and transform
    raw, reference = extract(spark, args.input_root)
    tables = transform(raw, reference, args)

    # Check the reconciliation BEFORE writing: unbalanced data must never reach gold/
    summary = summarize(tables)
    if not summary["all_days_balanced"]:
        raise RuntimeError(f"Record counts don't reconcile: {summary}")

    # Load, then report what was written
    load(tables, args.output_root)
    logger.info("Daily KPIs written", extra={"fields": {"output_root": args.output_root, **summary}})
    return summary


def main(argv: list[str] | None = None) -> None:
    """Entry point, the same on Glue and on a laptop."""
    run(build_spark(), parse_args(argv))


if __name__ == "__main__":
    main()
