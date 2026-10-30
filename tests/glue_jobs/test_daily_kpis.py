"""Checks of the job entry point: arguments as Glue passes them, and a full local run written to disk."""

from __future__ import annotations

from pathlib import Path

from src.glue_jobs import daily_kpis


def test_parse_args_ignores_glue_extras() -> None:
    """Glue adds --JOB_ID, --TempDir... to every run; the job must accept and ignore them."""
    args = daily_kpis.parse_args(
        [
            "--JOB_ID", "j_123",
            "--input_root", "s3://lake/",
            "--output_root", "s3://lake",
            "--utc_offset_hours", "-6",
            "--shift_start_hours", '{"MORNING": 6, "EVENING": 14, "NIGHT": 22}',
            "--coverage_threshold_days", "1.0",
            "--TempDir", "s3://tmp/",
        ]
    )  # fmt: skip

    assert args.input_root == "s3://lake"  # trailing slash removed
    assert args.utc_offset_hours == -6
    assert args.shift_start_hours["NIGHT"] == 22


def test_full_run_on_disk_is_idempotent(spark, history, lake_input, job_args, tmp_path: Path) -> None:
    """Run the job twice on a folder laid out like the bucket: same tables, no duplicated rows."""
    _simulator, events = history
    args = daily_kpis.JobArgs(
        input_root=str(lake_input),
        output_root=str(tmp_path / "lake"),
        utc_offset_hours=job_args.utc_offset_hours,
        shift_start_hours=job_args.shift_start_hours,
        coverage_threshold_days=job_args.coverage_threshold_days,
    )

    first = daily_kpis.run(spark, args)
    second = daily_kpis.run(spark, args)

    # Every raw record reconciled, and the second run replaced the partitions instead of appending
    assert first["all_days_balanced"] and first["raw_records"] == len(events)
    written = spark.read.parquet(str(tmp_path / "lake" / "gold" / "reject_rate"))
    assert written.count() == first["rows_per_table"]["gold/reject_rate"]
    assert second["rows_per_table"] == first["rows_per_table"]
