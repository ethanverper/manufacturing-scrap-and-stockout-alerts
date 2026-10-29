"""Shared fixtures: a simulated AWS (moto) with the Lambda's three tables, seeded like a real deploy.

moto intercepts every boto3 call inside `mock_aws()`, so the tests exercise the real DynamoDB API
(transactions, conditions, TTL fields) without an AWS account, credentials or cost.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterator
from datetime import date

import boto3
import pytest
from moto import mock_aws

from scripts.seed_stock import snapshot_to_items, write_items
from src.common.settings import settings
from src.generators.history import simulate_history
from src.lambdas.realtime_alerts import rules
from src.lambdas.realtime_alerts.state import StateStore, TableNames

TABLES = TableNames(events_seen="events_seen", part_stock="part_stock", station_hourly="station_hourly")
END_DAY = date(2026, 10, 27)


def _create_tables(dynamodb) -> None:
    """Create the three tables with the same keys Terraform will create (on-demand billing)."""
    specs = {
        TABLES.events_seen: [("dedupe_key", "HASH")],
        TABLES.part_stock: [("plant_part", "HASH")],
        TABLES.station_hourly: [("plant_station", "HASH"), ("hour", "RANGE")],
    }
    for name, keys in specs.items():
        dynamodb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": attribute, "KeyType": key_type} for attribute, key_type in keys],
            AttributeDefinitions=[{"AttributeName": attribute, "AttributeType": "S"} for attribute, _ in keys],
            BillingMode="PAY_PER_REQUEST",
        )


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fake credentials and region, so boto3 never even tries to reach real AWS."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    with mock_aws():
        yield


@pytest.fixture
def dynamodb(aws_env: None):
    """A moto DynamoDB resource with the three tables, part_stock seeded from the end of the history."""
    resource = boto3.resource("dynamodb", region_name="us-east-1")
    _create_tables(resource)

    # Seed exactly like `make seed`: the simulator's end-of-history stock, through the real script
    simulator, _ = simulate_history(days=7, end_day=END_DAY, seed=42)
    buffer = io.StringIO()
    rows = simulator.stock_snapshot_rows()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    buffer.seek(0)
    write_items(resource.Table(TABLES.part_stock), snapshot_to_items(list(csv.DictReader(buffer))))
    return resource


@pytest.fixture
def store(dynamodb) -> StateStore:
    """A StateStore on the moto tables, with a fixed clock (TTL values become predictable)."""
    return StateStore(client=dynamodb.meta.client, tables=TABLES, events_ttl_days=2, now=lambda: 1_800_000_000)


@pytest.fixture
def thresholds() -> rules.AlertThresholds:
    """The thresholds from config/settings.yaml, as Terraform will pass them."""
    return rules.AlertThresholds(**settings()["alerts"])
