"""Run the KPI queries and save the README charts (and each query's result as CSV).

Two engines, the same SQL (sql/):
    --engine local   Spark on data/lake-local (after `make kpis-local`): no AWS, $0
    --engine athena  the real gold tables (after `make deploy` and `make kpis`): ~$0.00 per run

Usage:
    python -m scripts.make_charts --engine local  --out data/charts
    python -m scripts.make_charts --engine athena --out docs/images --workgroup <wg> --database <db>
"""

from __future__ import annotations

import argparse
import csv
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.analytics import charts
from src.analytics.queries import list_queries, load_query
from src.common.logging_utils import get_logger
from src.common.settings import settings

logger = get_logger(__name__)

# Chart file name -> (query it reads, function that draws it). Coverage needs the threshold too.
CHARTS: dict[str, tuple[str, Callable[..., Any]]] = {
    "reject_rate_by_supplier_lot.png": ("02_reject_rate_by_supplier_lot", charts.supplier_lot_chart),
    "stock_coverage.png": ("03_stock_coverage", charts.coverage_chart),
    "throughput_vs_plan.png": ("04_throughput_vs_plan", charts.throughput_chart),
    "scrap_cost_by_supplier.png": ("05_scrap_cost_by_supplier", charts.scrap_cost_chart),
}


def build_runner(args: argparse.Namespace) -> Callable[[str], list[dict]]:
    """Create the query runner for the chosen engine; returns its `run(sql)` method."""
    # Local: the KPI job's own Spark settings (UTC), and every table it wrote
    if args.engine == "local":
        from src.analytics.local_spark import LocalSparkRunner
        from src.glue_jobs.daily_kpis import build_spark

        runner = LocalSparkRunner(build_spark("kpi-queries-local"))
        runner.register_lake(Path(args.lake))
        return runner.run

    # Athena: the deployed workgroup and database (names from `terraform output`)
    from src.analytics.athena import AthenaRunner
    from src.common.aws import get_client

    return AthenaRunner(
        client=get_client("athena"),
        workgroup=args.workgroup,
        database=args.database,
        timeout_seconds=settings()["aws"]["athena"]["query_timeout_seconds"],
    ).run


def write_csv(path: Path, rows: list[dict]) -> None:
    """Save a query's rows as CSV (the numbers behind each chart, for anyone to check)."""
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point: run every query, save its CSV, draw every chart."""
    # Engine and where to write
    parser = argparse.ArgumentParser(description="Run the KPI queries and draw the README charts.")
    parser.add_argument("--engine", choices=("local", "athena"), default="local")
    parser.add_argument("--lake", default="data/lake-local", help="local engine: output of make kpis-local")
    parser.add_argument("--workgroup", help="athena engine: terraform output athena_workgroup")
    parser.add_argument("--database", help="athena engine: terraform output athena_database")
    parser.add_argument("--out", default="data/charts", help="folder for the PNGs and CSVs")
    args = parser.parse_args(argv)
    out = Path(args.out)
    (out / "results").mkdir(parents=True, exist_ok=True)

    # Run every query once, keep its rows for the charts, and save them as CSV
    run = build_runner(args)
    results = {}
    for name in list_queries():
        results[name] = run(load_query(name))
        write_csv(out / "results" / f"{name}.csv", results[name])

    # Draw each chart from its query's rows
    threshold = settings()["alerts"]["stock_coverage_threshold_days"]
    for file_name, (query, draw) in CHARTS.items():
        figure = draw(results[query], threshold) if draw is charts.coverage_chart else draw(results[query])
        figure.savefig(out / file_name, dpi=150)

    logger.info("Charts written", extra={"fields": {"engine": args.engine, "out": str(out), "charts": list(CHARTS)}})


if __name__ == "__main__":
    main()
