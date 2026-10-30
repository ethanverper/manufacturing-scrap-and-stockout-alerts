"""The sql/ queries, run with Spark SQL on the gold tables the job builds (the same text runs on Athena).

Checks that every query runs, that ROLLUP subtotals add up level by level, that the planted story
reads correctly in the results, and that the README charts can be drawn from them.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.analytics import charts
from src.analytics.local_spark import LocalSparkRunner
from src.analytics.queries import list_queries, load_query


@pytest.fixture(scope="module")
def results(spark, tables) -> dict[str, list[dict]]:
    """Every query's rows, with the job's tables registered under their Athena names."""
    runner = LocalSparkRunner(spark)
    for folder, frame in tables.items():
        runner.register(folder, frame)
    return {name: runner.run(load_query(name)) for name in list_queries()}


def _level(rows: list[dict], level: str) -> list[dict]:
    """Rows of one hierarchy level."""
    return [row for row in rows if row["level"] == level]


def test_every_query_returns_rows(results) -> None:
    """All six queries parse and return data with Spark SQL."""
    assert len(results) == 6
    assert all(results[name] for name in results)


def test_rollup_levels_add_up(results, tables) -> None:
    """The grand total equals the sum of the plant rows, which equals the gold table itself."""
    rows = results["01_reject_rate_by_location"]
    total = _level(rows, "0 All plants")[0]["rejected_units"]
    plants = sum(row["rejected_units"] for row in _level(rows, "1 Plant"))
    table = tables["gold/reject_rate"].agg(F.sum("rejected_units")).first()[0]

    assert total == plants == table


def test_bad_lot_is_the_worst_lot(results) -> None:
    """Supplier → Part → Lot: LOT-7781 tops the lot level, far above the overall rate."""
    rows = results["02_reject_rate_by_supplier_lot"]
    worst = max(_level(rows, "3 Lot"), key=lambda row: row["supplier_defect_rate_pct"])
    overall = _level(rows, "0 All suppliers")[0]["supplier_defect_rate_pct"]

    assert worst["lot"] == "LOT-7781"
    assert worst["supplier_defect_rate_pct"] > 10 * overall


def test_line_3_night_is_the_only_shift_behind(results) -> None:
    """Plant → Line → Shift: in both plants, only LINE-3 NIGHT is below 95% of plan."""
    behind = {(row["plant"], row["line"], row["shift"]) for row in _level(results["04_throughput_vs_plan"], "3 Shift")
              if row["throughput_pct"] < 95}  # fmt: skip

    assert behind == {("PLANT-MTY", "LINE-3", "NIGHT"), ("PLANT-SLP", "LINE-3", "NIGHT")}


def test_scrap_cost_splits_into_recoverable_and_internal(results) -> None:
    """Finance check: recoverable + internal loss = total scrap cost, at the grand total."""
    total = _level(results["05_scrap_cost_by_supplier"], "0 All plants")[0]

    assert total["recoverable_usd"] + total["internal_loss_usd"] == pytest.approx(total["scrap_cost_usd"], abs=0.05)


def test_data_quality_is_balanced_every_day(results) -> None:
    """The reconciliation is visible to analysts too: every day balanced."""
    assert all(row["is_balanced"] for row in results["06_data_quality"])


def test_charts_are_drawn(results, tmp_path: Path) -> None:
    """Each README chart renders from its query's rows to a non-empty PNG."""
    figures = {
        "lots.png": charts.supplier_lot_chart(results["02_reject_rate_by_supplier_lot"]),
        "coverage.png": charts.coverage_chart(results["03_stock_coverage"], threshold_days=1.0),
        "throughput.png": charts.throughput_chart(results["04_throughput_vs_plan"]),
        "scrap.png": charts.scrap_cost_chart(results["05_scrap_cost_by_supplier"]),
    }
    for name, figure in figures.items():
        figure.savefig(tmp_path / name)

    assert all((tmp_path / name).stat().st_size > 10_000 for name in figures)
