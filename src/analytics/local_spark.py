"""Run the sql/ queries with Spark on the lake built locally by `make kpis-local` (no AWS).

Why: the same SQL text runs on Athena in AWS. Running it locally first catches mistakes for free,
and lets the charts be drawn before any deploy. The queries stick to SQL both engines accept
(CASE instead of engine-specific GROUPING() functions, NULLIF, ROLLUP, NULLS FIRST).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def table_name(folder: str) -> str:
    """Athena table name for a lake folder: "/" becomes "_" (the convention in 5_analytics.tf).

    Example: "gold/reject_rate" -> "gold_reject_rate".
    """
    return folder.strip("/").replace("/", "_")


class LocalSparkRunner:
    """Registers lake tables as Spark views with their Athena names, then runs SQL on them."""

    def __init__(self, spark: SparkSession) -> None:
        """Wrap a Spark session (use the UTC session from daily_kpis.build_spark)."""
        self.spark = spark

    def register(self, folder: str, frame: DataFrame) -> None:
        """Expose a DataFrame under the Athena name of its lake folder.

        Args:
            folder: Lake folder, e.g. "gold/reject_rate".
            frame: The table's data.
        """
        frame.createOrReplaceTempView(table_name(folder))

    def register_lake(self, lake_root: Path) -> list[str]:
        """Register every Parquet table written by the KPI job under a local lake folder.

        Args:
            lake_root: Output folder of `make kpis-local` (e.g. data/lake-local).

        Returns:
            The registered table names.
        """
        # Keep dt as text, like the Athena tables declare it (Spark would otherwise guess "date")
        self.spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

        names = []
        # Every table is two levels deep (layer/table), e.g. gold/reject_rate or curated/events_clean
        for table_dir in sorted(path for path in lake_root.glob("*/*") if path.is_dir()):
            folder = table_dir.relative_to(lake_root).as_posix()
            self.register(folder, self.spark.read.parquet(str(table_dir)))
            names.append(table_name(folder))
        return names

    def run(self, sql: str) -> list[dict[str, Any]]:
        """Run one query and return its rows as plain dicts (the same shape AthenaRunner returns)."""
        return [row.asDict() for row in self.spark.sql(sql).collect()]
