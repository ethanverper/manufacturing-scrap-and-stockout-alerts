"""Shared fixtures for the Glue job tests: a local Spark session and the simulated plant data.

The tests run the real PySpark code of src/glue_jobs/ on a laptop (or in CI): no Glue, no S3, no
cost. They are skipped when PySpark isn't installed, so the rest of the suite still runs anywhere.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.common.settings import settings
from src.generators import history as history_module
from src.generators.history import simulate_history

# Skip this whole folder if PySpark is missing (it needs Java too; CI runners have both)
pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402  (only importable after the skip check)

from src.glue_jobs import daily_kpis, transforms  # noqa: E402

# The same history the other test folders use
END_DAY = date(2026, 10, 27)
HISTORY_DAYS = 7
SEED = 42


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """One small local Spark session for every test (starting the JVM takes seconds)."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("glue-job-tests")
        # The same two settings as the real job (daily_kpis.build_spark)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Small data: few shuffle partitions and no web UI keep the tests fast
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def history() -> tuple:
    """The simulated 7-day history: (simulator at the end of the history, noisy raw events)."""
    return simulate_history(days=HISTORY_DAYS, end_day=END_DAY, seed=SEED)


@pytest.fixture(scope="session")
def job_args() -> daily_kpis.JobArgs:
    """The job arguments Terraform would pass, built from config/settings.yaml."""
    config = settings()
    return daily_kpis.JobArgs(
        input_root="unused",
        output_root="unused",
        utc_offset_hours=config["project"]["plant_utc_offset_hours"],
        shift_start_hours=config["plant"]["shift_start_hours"],
        coverage_threshold_days=config["alerts"]["stock_coverage_threshold_days"],
    )


@pytest.fixture(scope="session")
def lake_input(history: tuple, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A folder laid out exactly like the S3 bucket (raw/ and reference/), as `make generate` writes it.

    Why files and not in-memory rows: the tests then also exercise the job's real reader
    (declared schemas, gzipped JSON lines, CSV headers), and Spark reads files much faster.
    """
    simulator, events = history
    root = tmp_path_factory.mktemp("lake_input")
    history_module.write_raw_events(events, root)
    plan_days = [simulator.calendar_day(day) for day in range(HISTORY_DAYS + 1)]
    history_module.write_reference_data(root, plan_days, history_module.opening_stock_rows(HISTORY_DAYS, END_DAY, SEED))
    return root


@pytest.fixture(scope="session")
def extracted(spark: SparkSession, lake_input: Path) -> tuple[DataFrame, dict[str, DataFrame]]:
    """(raw events, reference tables), read with the job's own extract()."""
    raw, reference_tables = daily_kpis.extract(spark, str(lake_input))
    return raw.cache(), {name: frame.cache() for name, frame in reference_tables.items()}


@pytest.fixture(scope="session")
def reference_tables(extracted: tuple) -> dict[str, DataFrame]:
    """The reference tables (parts, stations, plan, opening_stock)."""
    return extracted[1]


def events_frame(spark: SparkSession, events: list[dict]) -> DataFrame:
    """Hand-made raw events as a DataFrame with the job's declared raw schema."""
    # Quantities are doubles in the declared schema
    rows = [{**event, "quantity": None if event["quantity"] is None else float(event["quantity"])} for event in events]
    return spark.createDataFrame(rows, schema=transforms.RAW_EVENT_SCHEMA)


@pytest.fixture(scope="session")
def tables(extracted: tuple, job_args) -> dict[str, DataFrame]:
    """Every output table of the job, computed once on the full history."""
    raw, reference_tables = extracted
    outputs = daily_kpis.transform(raw, reference_tables, job_args)
    return {path: frame.cache() for path, frame in outputs.items()}
