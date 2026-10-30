"""Checks of the PySpark transformations on the simulated history: clean data, right KPIs, the story.

Run locally with a small Spark session (tests/glue_jobs/conftest.py): no Glue, no S3, no cost.
"""

from __future__ import annotations

from collections import defaultdict

from pyspark.sql import functions as F

from src.generators import reference
from src.glue_jobs import daily_kpis, transforms
from tests.glue_jobs.conftest import events_frame

# A valid install scan to start the hand-made validation cases from
VALID_EVENT = {
    "event_id": "E-1",
    "event_time": "2026-10-27T15:00:00Z",
    "event_type": "CONSUMED",
    "plant": "PLANT-MTY",
    "line": "LINE-1",
    "station": "LINE-1-ASSEMBLY-1",
    "part": "BATTERY-A12",
    "supplier": "VOLTEX",
    "lot": "LOT-7001",
    "quantity": 5,
    "reason_code": None,
}


def _unique_valid_events(events: list[dict]) -> dict[str, dict]:
    """The clean events computed in plain Python, as a reference: corrupted scans out, retries once."""
    clean: dict[str, dict] = {}
    for event in events:
        if event["event_id"].endswith("-X"):
            continue
        clean.setdefault(event["event_id"], event)
    return clean


# ---------------------------------------------------------------------------
# Validation, deduplication and record reconciliation
# ---------------------------------------------------------------------------


def test_validate_names_the_first_broken_rule(spark, reference_tables, job_args) -> None:
    """Each kind of bad record is quarantined with its own reason; the good one passes."""
    cases = {
        "ok": {},
        "non_positive_quantity": {"quantity": -5},
        "unknown_part": {"part": "UNKNOWN-PART"},
        "missing_plant": {"plant": None},
        "unknown_station": {"station": "LINE-9-ASSEMBLY-1"},
        "unknown_event_type": {"event_type": "TELEPORTED"},
        "invalid_event_time": {"event_time": "yesterday"},
    }
    rows = [{**VALID_EVENT, **changes, "event_id": name} for name, changes in cases.items()]
    events = transforms.add_event_columns(
        events_frame(spark, rows), job_args.utc_offset_hours, job_args.shift_start_hours
    )

    valid, quarantine = transforms.validate(events, reference_tables["parts"], reference_tables["stations"])

    assert [row["event_id"] for row in valid.collect()] == ["ok"]
    reasons = {row["event_id"]: row["quarantine_reason"] for row in quarantine.collect()}
    assert reasons == {name: name for name in cases if name != "ok"}


def test_quarantine_holds_exactly_the_corrupted_scans(history, tables) -> None:
    """The simulator marks corrupted scans with an id ending in -X: those, and only those, are quarantined."""
    _simulator, events = history
    corrupted = {event["event_id"] for event in events if event["event_id"].endswith("-X")}

    quarantined = {row["event_id"] for row in tables["quarantine/plant_events"].select("event_id").collect()}

    assert corrupted and quarantined == corrupted


def test_curated_counts_every_scan_exactly_once(history, tables) -> None:
    """Scanner retries are removed: one curated row per event_id, matching a plain-Python count."""
    _simulator, events = history
    curated = tables["curated/events_clean"]

    assert curated.count() == len(_unique_valid_events(events))
    assert curated.select("event_id").distinct().count() == curated.count()


def test_every_raw_record_is_accounted_for(history, tables) -> None:
    """raw = curated + duplicates + quarantined, on every day (nothing silently lost)."""
    _simulator, events = history
    quality = tables["gold/data_quality"].collect()

    assert all(row["is_balanced"] for row in quality)
    assert sum(row["raw_records"] for row in quality) == len(events)
    assert sum(row["duplicate_records"] for row in quality) > 0  # the history has retries to remove


# ---------------------------------------------------------------------------
# KPIs and the story planted in the data
# ---------------------------------------------------------------------------


def test_closing_stock_reconciles_with_the_plant(history, tables) -> None:
    """Batch vs real time: opening stock + every movement = the stock the simulator ends with.

    The simulator's final stock is what seeds DynamoDB for the real-time path, so this proves both
    paths start the live session from the same numbers.
    """
    simulator, _events = history
    coverage = tables["gold/stock_coverage"]
    last_dt = coverage.agg(F.max("dt")).first()[0]

    batch = {
        (row["plant"], row["part"]): row["closing_units"] for row in coverage.filter(F.col("dt") == last_dt).collect()
    }
    plant = {(row["plant"], row["part"]): row["on_hand"] for row in simulator.stock_snapshot_rows()}

    assert batch.keys() == plant.keys()
    assert all(abs(batch[key] - plant[key]) < 0.5 for key in plant)


def test_bad_lot_stands_out_in_reject_rate(tables) -> None:
    """Supplier → Part → Lot: LOT-7781 of BATTERY-A12 rejects far more than any other lot."""
    by_lot = (
        tables["gold/reject_rate"]
        .filter((F.col("plant") == "PLANT-MTY") & (F.col("part") == "BATTERY-A12"))
        .groupBy("lot")
        .agg(F.sum("consumed_units").alias("consumed"), F.sum("supplier_defect_units").alias("defects"))
        .collect()
    )
    rates = {row["lot"]: row["defects"] / (row["consumed"] + row["defects"]) for row in by_lot}

    assert rates["LOT-7781"] > 0.10
    assert all(rate < 0.03 for lot, rate in rates.items() if lot != "LOT-7781")


def test_scrap_cost_recoverable_matches_a_manual_count(history, tables) -> None:
    """Recoverable scrap per supplier = supplier-defect units × unit cost, counted in plain Python."""
    _simulator, events = history
    unit_cost = {part.part_id: part.unit_cost for part in reference.PARTS.values()}
    expected: dict[str, float] = defaultdict(float)
    for event in _unique_valid_events(events).values():
        if event["reason_code"] == "SUPPLIER_DEFECT":
            expected[event["supplier"]] += event["quantity"] * unit_cost[event["part"]]

    actual = {
        row["supplier"]: row["cost"]
        for row in tables["gold/scrap_cost"]
        .filter("is_recoverable")
        .groupBy("supplier")
        .agg(F.sum("scrap_cost_usd").alias("cost"))
        .collect()
    }

    assert actual.keys() == expected.keys()
    assert all(abs(actual[supplier] - expected[supplier]) < 1.0 for supplier in expected)


def test_line_3_night_shift_is_behind_plan(tables) -> None:
    """Plant → Line → Shift: LINE-3 on the night shift produces clearly less than planned."""
    rows = (
        tables["gold/throughput"]
        .groupBy("line", "shift")
        .agg(F.sum("produced_units").alias("produced"), F.sum("planned_units").alias("planned"))
        .collect()
    )
    pct = {(row["line"], row["shift"]): row["produced"] / row["planned"] for row in rows}

    assert pct[("LINE-3", "NIGHT")] < 0.92
    assert all(value > 0.97 for key, value in pct.items() if key != ("LINE-3", "NIGHT"))


def test_connector_shortage_shows_in_coverage(tables) -> None:
    """CONNECTOR-C4 in Monterrey ends the history with less coverage than in San Luis Potosí."""
    coverage = tables["gold/stock_coverage"]
    last_dt = coverage.agg(F.max("dt")).first()[0]
    c4 = {
        row["plant"]: row["coverage_days"]
        for row in coverage.filter((F.col("dt") == last_dt) & (F.col("part") == "CONNECTOR-C4")).collect()
    }

    assert c4["PLANT-MTY"] < c4["PLANT-SLP"]


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_replaying_every_event_changes_nothing(extracted, job_args, tables) -> None:
    """If Firehose or a backfill delivered everything twice, the KPIs would be identical."""
    raw, reference_tables = extracted
    replayed = daily_kpis.transform(raw.unionByName(raw), reference_tables, job_args)

    def sorted_rows(frame):
        return sorted(tuple(row) for row in frame.collect())

    assert sorted_rows(replayed["gold/reject_rate"]) == sorted_rows(tables["gold/reject_rate"])
    assert sorted_rows(replayed["gold/stock_coverage"]) == sorted_rows(tables["gold/stock_coverage"])
