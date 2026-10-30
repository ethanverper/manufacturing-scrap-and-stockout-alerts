"""Run the Glue KPI job on this laptop, on the generated data, with a local Spark (no AWS).

Why a separate script:
    On AWS, Terraform passes the job its arguments from config/settings.yaml. Locally, this script
    does the same thing: it reads the settings and calls the very same job code, so what is tested
    here is what runs on Glue.

Usage (after `make generate`):
    python -m scripts.kpis_local --input data/generated --output data/lake-local
"""

from __future__ import annotations

import argparse
import json

from src.common.settings import settings
from src.glue_jobs import daily_kpis


def job_argv(input_root: str, output_root: str) -> list[str]:
    """Build the same arguments Terraform gives the Glue job (infra/terraform/4_batch.tf).

    Args:
        input_root: Folder with raw/ and reference/.
        output_root: Folder for curated/, gold/ and quarantine/.

    Returns:
        The command-line arguments for daily_kpis.parse_args.
    """
    config = settings()
    return [
        "--input_root", input_root,
        "--output_root", output_root,
        "--utc_offset_hours", str(config["project"]["plant_utc_offset_hours"]),
        "--shift_start_hours", json.dumps(config["plant"]["shift_start_hours"]),
        "--coverage_threshold_days", str(config["alerts"]["stock_coverage_threshold_days"]),
    ]  # fmt: skip


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point: run the job locally and print its summary."""
    # Where to read and write
    parser = argparse.ArgumentParser(description="Run the daily KPI job locally.")
    parser.add_argument("--input", default="data/generated", help="folder with raw/ and reference/")
    parser.add_argument("--output", default="data/lake-local", help="folder for the output tables")
    args = parser.parse_args(argv)

    # The job's own entry point, with the settings turned into job arguments
    spark = daily_kpis.build_spark("daily-kpis-local")
    summary = daily_kpis.run(spark, daily_kpis.parse_args(job_argv(args.input, args.output)))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
