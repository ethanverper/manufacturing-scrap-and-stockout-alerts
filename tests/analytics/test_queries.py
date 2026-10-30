"""The sql/ folder: every query is found, loads cleanly, and avoids engine-specific SQL."""

from __future__ import annotations

from src.analytics.queries import list_queries, load_query


def test_queries_are_listed_in_order() -> None:
    """One file per business question, numbered."""
    assert list_queries() == [
        "01_reject_rate_by_location",
        "02_reject_rate_by_supplier_lot",
        "03_stock_coverage",
        "04_throughput_vs_plan",
        "05_scrap_cost_by_supplier",
        "06_data_quality",
    ]


def test_queries_are_portable_between_athena_and_spark() -> None:
    """No trailing semicolon (both APIs reject it) and no GROUPING(), whose meaning differs by engine."""
    for name in list_queries():
        sql = load_query(name)
        assert not sql.endswith(";")
        assert "GROUPING(" not in sql.upper()
