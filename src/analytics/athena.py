"""Run a query in the project's Athena workgroup and return plain Python rows.

Where it sits in the pipeline:
    scripts/make_charts.py --engine athena uses it after `make deploy` and `make kpis`, to run the
    queries in sql/ against the real gold tables and draw the README charts.

How Athena runs a query (asynchronous, three API calls):
    1. start_query_execution  → Athena returns an id right away and runs the query in the background
    2. get_query_execution    → poll the state until SUCCEEDED, FAILED or CANCELLED
    3. get_query_results      → read the rows (paginated; the first row is the header)
Cost: $5 per TB scanned. The workgroup cancels any query above 1 GB (infra/terraform/5_analytics.tf).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.common.logging_utils import get_logger

logger = get_logger(__name__)

# Final states of a query execution
SUCCEEDED = "SUCCEEDED"
FINISHED_STATES = (SUCCEEDED, "FAILED", "CANCELLED")

# Athena's price, used only to log what each query cost
USD_PER_TB_SCANNED = 5.0
BYTES_PER_TB = 1024**4

# Athena column types that become int, float or bool in Python (everything else stays a string)
INTEGER_TYPES = {"tinyint", "smallint", "integer", "bigint"}
FLOAT_TYPES = {"double", "float", "real", "decimal"}


class QueryFailedError(RuntimeError):
    """The query finished in FAILED or CANCELLED state, or took longer than the timeout."""


def convert_value(value: str | None, athena_type: str) -> Any:
    """Turn one cell (Athena returns every cell as text) into the matching Python type.

    Args:
        value: The cell text, or None for SQL NULL.
        athena_type: The column type from the result metadata, e.g. "double".

    Returns:
        int, float, bool, str or None.
    """
    # SQL NULL arrives as a missing value
    if value is None:
        return None

    # Numbers and booleans by declared type
    if athena_type in INTEGER_TYPES:
        return int(value)
    if athena_type in FLOAT_TYPES:
        return float(value)
    if athena_type == "boolean":
        return value.lower() == "true"
    return value


def parse_result_pages(pages: list[dict]) -> list[dict[str, Any]]:
    """Turn the pages of get_query_results into a list of {column: value} rows.

    Pure function, so it is tested without AWS (tests/analytics/test_athena.py).

    Args:
        pages: Responses of get_query_results, in order.

    Returns:
        One dict per data row (the header row is skipped).
    """
    # Column names and types come once, in the first page's metadata
    if not pages:
        return []
    columns = pages[0]["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    names = [column["Name"] for column in columns]
    types = [column["Type"] for column in columns]

    # All rows of all pages; the very first row repeats the column names, so skip it
    raw_rows = [row for page in pages for row in page["ResultSet"]["Rows"]][1:]

    # Each cell is {"VarCharValue": "..."}, or {} when the value is NULL
    return [
        {
            name: convert_value(cell.get("VarCharValue"), athena_type)
            for name, athena_type, cell in zip(names, types, row["Data"], strict=True)
        }
        for row in raw_rows
    ]


@dataclass
class AthenaRunner:
    """Runs SQL in one workgroup and database.

    Attributes:
        client: A boto3 Athena client (tests pass a fake).
        workgroup: The project's workgroup (terraform output athena_workgroup).
        database: The catalog database (terraform output athena_database).
        timeout_seconds: Give up waiting after this long (config: aws.athena.query_timeout_seconds).
        poll_seconds: Time between state checks.
        sleep: Injectable so tests don't actually wait.
    """

    client: Any
    workgroup: str
    database: str
    timeout_seconds: float = 120.0
    poll_seconds: float = 1.0
    sleep: Callable[[float], None] = field(default=time.sleep)

    def run(self, sql: str) -> list[dict[str, Any]]:
        """Run one query and return its rows.

        Raises:
            QueryFailedError: The query failed, was cancelled, or timed out.
        """
        # 1. Start the query in the workgroup (which also sets the results folder and the scan limit)
        execution_id = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        # 2. Wait for it to finish, then 3. read every page of results
        self._wait(execution_id)
        pages = list(self.client.get_paginator("get_query_results").paginate(QueryExecutionId=execution_id))
        return parse_result_pages(pages)

    def _wait(self, execution_id: str) -> None:
        """Poll until the query finishes; log what it scanned and cost.

        Raises:
            QueryFailedError: On FAILED, CANCELLED or timeout.
        """
        waited = 0.0
        while True:
            # Current state of the query
            execution = self.client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
            state = execution["Status"]["State"]

            # Finished: log the bill, and fail loudly unless it succeeded
            if state in FINISHED_STATES:
                scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
                logger.info(
                    "Athena query finished",
                    extra={
                        "fields": {
                            "execution_id": execution_id,
                            "state": state,
                            "bytes_scanned": scanned,
                            "cost_usd": round(scanned / BYTES_PER_TB * USD_PER_TB_SCANNED, 6),
                        }
                    },
                )
                if state != SUCCEEDED:
                    reason = execution["Status"].get("StateChangeReason", "no reason given")
                    raise QueryFailedError(f"Query {execution_id} {state}: {reason}")
                return

            # Still queued or running: stop waiting after the timeout, otherwise check again soon
            if waited >= self.timeout_seconds:
                raise QueryFailedError(f"Query {execution_id} still {state} after {self.timeout_seconds} s")
            self.sleep(self.poll_seconds)
            waited += self.poll_seconds
