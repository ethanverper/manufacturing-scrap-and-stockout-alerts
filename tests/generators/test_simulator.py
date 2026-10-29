"""End-to-end checks on simulated data: reproducible, valid, and telling the planted story."""

from __future__ import annotations

import random
from datetime import date

import pytest

from src.generators.events import EVENT_TYPES, REJECTED, SUPPLIER_DEFECT
from src.generators.history import simulate_history
from src.generators.simulator import PlantSimulator, add_scanner_noise

END_DAY = date(2026, 10, 27)


@pytest.fixture(scope="module")
def history() -> tuple[PlantSimulator, list[dict]]:
    """Seven simulated days, shared by the tests in this module (simulating once keeps them fast)."""
    return simulate_history(days=7, end_day=END_DAY, seed=42)


def _unique_valid(events: list[dict]) -> list[dict]:
    """Drop retries and corrupted scans, the way the pipeline will."""
    seen: set[str] = set()
    clean = []
    for event in events:
        if event["event_id"] in seen or not event["plant"] or event["quantity"] <= 0 or event["part"] == "UNKNOWN-PART":
            continue
        seen.add(event["event_id"])
        clean.append(event)
    return clean


def test_same_seed_same_events() -> None:
    """Reproducibility: the README numbers must come out the same on every run."""
    first = PlantSimulator(history_days=7, first_day=date(2026, 10, 21), seed=1).simulate_day(0, range(2))
    second = PlantSimulator(history_days=7, first_day=date(2026, 10, 21), seed=1).simulate_day(0, range(2))
    assert first == second


def test_events_follow_the_schema(history: tuple[PlantSimulator, list[dict]]) -> None:
    """Every event has every field and a known type; rejects always carry a reason."""
    _, events = history
    expected_keys = {
        "event_id",
        "event_time",
        "event_type",
        "plant",
        "line",
        "station",
        "part",
        "supplier",
        "lot",
        "quantity",
        "reason_code",
    }
    for event in _unique_valid(events):
        assert set(event) == expected_keys
        assert event["event_type"] in EVENT_TYPES
        assert (event["reason_code"] is not None) == (event["event_type"] == REJECTED)


def test_bad_lot_raises_battery_rejects_at_monterrey(history: tuple[PlantSimulator, list[dict]]) -> None:
    """Story 1: once LOT-7781 is in use, supplier rejects on BATTERY-A12 jump well above the baseline."""
    _, events = history
    in_lot = [e for e in _unique_valid(events) if e["lot"] == "LOT-7781" and e["event_type"] in ("CONSUMED", REJECTED)]
    consumed = sum(e["quantity"] for e in in_lot if e["event_type"] == "CONSUMED")
    supplier_rejects = sum(e["quantity"] for e in in_lot if e["reason_code"] == SUPPLIER_DEFECT)
    assert consumed > 0
    assert supplier_rejects / (consumed + supplier_rejects) > 0.10


def test_connector_shortage_ends_history_just_above_one_day(history: tuple[PlantSimulator, list[dict]]) -> None:
    """Story 2: the live session must start above 1 day of coverage and cross below it."""
    simulator, _ = history
    row = next(r for r in simulator.stock_snapshot_rows() if (r["plant"], r["part"]) == ("PLANT-MTY", "CONNECTOR-C4"))
    assert 1.0 < row["coverage_days"] < 1.15


def test_noise_adds_duplicates_and_invalid_records() -> None:
    """The pipeline must have real duplicates and corrupted scans to handle."""
    events = PlantSimulator(history_days=7, first_day=date(2026, 10, 21), seed=3).simulate_day(0, range(3))
    noisy = add_scanner_noise(events, random.Random(0), duplicate_rate=0.05, invalid_rate=0.05)
    ids = [event["event_id"] for event in noisy]
    assert len(ids) > len(set(ids))  # duplicates share an id
    assert any(event["event_id"].endswith("-X") for event in noisy)  # corrupted copies
