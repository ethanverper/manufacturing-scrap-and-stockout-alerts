"""The Athena runner, against a fake client: result parsing, waiting, failures and timeouts. No AWS."""

from __future__ import annotations

import pytest

from src.analytics.athena import AthenaRunner, QueryFailedError, convert_value, parse_result_pages


def _page(rows: list[list[str | None]], columns: list[tuple[str, str]] | None = None) -> dict:
    """Build one get_query_results page (Athena sends every cell as text, NULL as an empty cell)."""
    page = {"ResultSet": {"Rows": [{"Data": [{} if v is None else {"VarCharValue": v} for v in row]} for row in rows]}}
    if columns:
        page["ResultSet"]["ResultSetMetadata"] = {"ColumnInfo": [{"Name": n, "Type": t} for n, t in columns]}
    return page


COLUMNS = [("plant", "varchar"), ("rejected_units", "double"), ("days", "bigint"), ("is_balanced", "boolean")]


class FakeAthena:
    """Just enough of the boto3 Athena client: states to return in order, then the result pages."""

    def __init__(self, states: list[str], pages: list[dict]) -> None:
        self.states = list(states)
        self.pages = pages
        self.started_with: dict = {}

    def start_query_execution(self, **kwargs) -> dict:
        self.started_with = kwargs
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId: str) -> dict:  # noqa: N803 (boto3's argument name)
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": "SYNTAX_ERROR"},
                "Statistics": {"DataScannedInBytes": 2048},
            }
        }

    def get_paginator(self, name: str):
        pages = self.pages

        class Paginator:
            def paginate(self, **kwargs):
                return iter(pages)

        return Paginator()


def test_convert_value_by_type() -> None:
    """Text cells become numbers, booleans or None."""
    assert convert_value("4.5", "double") == 4.5
    assert convert_value("12", "bigint") == 12
    assert convert_value("true", "boolean") is True
    assert convert_value(None, "double") is None
    assert convert_value("PLANT-MTY", "varchar") == "PLANT-MTY"


def test_parse_skips_the_header_and_reads_every_page() -> None:
    """The first row repeats the column names; rows continue across pages; NULL stays None."""
    pages = [
        _page([["plant", "rejected_units", "days", "is_balanced"], [None, "10.0", "7", "true"]], COLUMNS),
        _page([["PLANT-MTY", "4.0", "7", "false"]]),
    ]

    assert parse_result_pages(pages) == [
        {"plant": None, "rejected_units": 10.0, "days": 7, "is_balanced": True},
        {"plant": "PLANT-MTY", "rejected_units": 4.0, "days": 7, "is_balanced": False},
    ]


def test_run_waits_until_the_query_succeeds() -> None:
    """QUEUED → RUNNING → SUCCEEDED, in the workgroup and database given."""
    client = FakeAthena(["QUEUED", "RUNNING", "SUCCEEDED"], [_page([["plant"], ["PLANT-SLP"]], [("plant", "varchar")])])
    runner = AthenaRunner(client, workgroup="wg", database="db", sleep=lambda _: None)

    assert runner.run("SELECT plant FROM t") == [{"plant": "PLANT-SLP"}]
    assert client.started_with["WorkGroup"] == "wg"
    assert client.started_with["QueryExecutionContext"] == {"Database": "db"}


def test_failed_query_raises_with_the_reason() -> None:
    """A FAILED query stops the script with Athena's reason, instead of drawing empty charts."""
    runner = AthenaRunner(FakeAthena(["FAILED"], []), workgroup="wg", database="db", sleep=lambda _: None)

    with pytest.raises(QueryFailedError, match="SYNTAX_ERROR"):
        runner.run("SELEC 1")


def test_slow_query_times_out() -> None:
    """A query still running after the timeout is abandoned."""
    runner = AthenaRunner(
        FakeAthena(["RUNNING"], []), workgroup="wg", database="db", timeout_seconds=3, sleep=lambda _: None
    )

    with pytest.raises(QueryFailedError, match="still RUNNING"):
        runner.run("SELECT 1")
