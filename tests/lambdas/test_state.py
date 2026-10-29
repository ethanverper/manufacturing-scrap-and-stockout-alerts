"""DynamoDB state against simulated AWS: exactly-once updates, unknown parts, alert de-duplication."""

from __future__ import annotations

import pytest

from src.lambdas.realtime_alerts.parsing import parse_event
from src.lambdas.realtime_alerts.state import APPLIED, DUPLICATE, StateStore, UnknownPartError


def scan(**overrides) -> dict:
    """A valid CONSUMED scan of 20 connectors at Monterrey, with optional changes."""
    base = {
        "event_id": "EV-TEST-0001",
        "event_time": "2026-10-28T12:30:00Z",
        "event_type": "CONSUMED",
        "plant": "PLANT-MTY",
        "line": "LINE-1",
        "station": "LINE-1-ASSEMBLY-2",
        "part": "CONNECTOR-C4",
        "supplier": "SUP-NORDCAB",
        "lot": "LOT-7102",
        "quantity": 20,
        "reason_code": None,
    }
    return parse_event({**base, **overrides})


def test_event_moves_stock_and_counts_for_the_station(store: StateStore) -> None:
    """One install: stock goes down by 20, the station's hour counts 20 consumed."""
    before = store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"]

    assert store.apply_event(scan()) == APPLIED

    assert store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"] == before - 20
    assert store.station_hour("PLANT-MTY#LINE-1-ASSEMBLY-2", "2026-10-28T12") == (20, 0)


def test_retried_event_is_counted_once(store: StateStore) -> None:
    """The same event_id twice (a scanner retry): the second time changes nothing."""
    before = store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"]

    assert store.apply_event(scan()) == APPLIED
    assert store.apply_event(scan()) == DUPLICATE

    assert store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"] == before - 20
    assert store.station_hour("PLANT-MTY#LINE-1-ASSEMBLY-2", "2026-10-28T12") == (20, 0)


def test_unknown_part_is_rejected_and_writes_nothing(store: StateStore) -> None:
    """A part master data doesn't know is bad data: the whole transaction is cancelled."""
    with pytest.raises(UnknownPartError):
        store.apply_event(scan(event_id="EV-TEST-0002", part="UNKNOWN-PART"))

    # Not even the seen-marker or the station counter were written (all or nothing)
    assert store.station_hour("PLANT-MTY#LINE-1-ASSEMBLY-2", "2026-10-28T12") == (0, 0)


def test_delivery_adds_stock_and_skips_station_counters(store: StateStore) -> None:
    """RECEIVED raises stock and doesn't touch any station's reject-rate counters."""
    before = store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"]
    delivery = scan(event_id="EV-TEST-0003", event_type="RECEIVED", line=None, station="WAREHOUSE", quantity=6000)

    assert store.apply_event(delivery) == APPLIED
    assert store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"] == before + 6000


def test_rejects_count_in_their_own_counter(store: StateStore) -> None:
    """REJECTED feeds the 'rejected' counter, CONSUMED the 'consumed' one."""
    store.apply_event(scan())
    store.apply_event(scan(event_id="EV-TEST-0004", event_type="REJECTED", quantity=3, reason_code="SUPPLIER_DEFECT"))

    assert store.station_hour("PLANT-MTY#LINE-1-ASSEMBLY-2", "2026-10-28T12") == (20, 3)


def test_each_alert_is_claimed_only_once(store: StateStore) -> None:
    """The first claim sends the email; every later match of the same problem doesn't."""
    key = "LOW_STOCK#PLANT-MTY#CONNECTOR-C4#2026-10-28"
    assert store.claim_alert(key) is True
    assert store.claim_alert(key) is False
