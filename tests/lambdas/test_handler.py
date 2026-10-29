"""The whole Lambda against simulated AWS, fed with the real live session."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field

import pytest

from src.generators.live import build_live_events
from src.lambdas.realtime_alerts import handler as handler_module
from src.lambdas.realtime_alerts.handler import Dependencies
from src.lambdas.realtime_alerts.rules import HIGH_REJECT_RATE, LOW_STOCK, Alert, AlertThresholds
from src.lambdas.realtime_alerts.state import StateStore

from .conftest import END_DAY

BATCH_SIZE = 100  # same as config/settings.yaml -> aws.lambda.batch_size


@dataclass
class CapturingNotifier:
    """Stands in for SNS and keeps every alert it was asked to send."""

    sent: list[Alert] = field(default_factory=list)

    def publish(self, alert: Alert) -> str:
        self.sent.append(alert)
        return f"msg-{len(self.sent)}"


def kinesis_batches(events: list[dict]) -> list[dict]:
    """Package events the way the event source mapping delivers them: batches of Kinesis records."""
    records = [
        {"kinesis": {"data": base64.b64encode((json.dumps(e) + "\n").encode()).decode(), "sequenceNumber": str(i)}}
        for i, e in enumerate(events)
    ]
    return [{"Records": records[i : i + BATCH_SIZE]} for i in range(0, len(records), BATCH_SIZE)]


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, store: StateStore, thresholds: AlertThresholds) -> CapturingNotifier:
    """Point the handler at the moto-backed store and a capturing notifier."""
    notifier = CapturingNotifier()
    monkeypatch.setattr(handler_module, "_dependencies", lambda: Dependencies(store, notifier, thresholds))
    return notifier


def test_live_session_sends_only_the_real_alerts(wired: CapturingNotifier) -> None:
    """End to end: ~1,000 events -> 1 low-stock alert + 7 reject alerts, all on the bad-lot stations."""
    events = build_live_events(history_days=7, end_day=END_DAY, seed=42)

    for batch in kinesis_batches(events):
        assert handler_module.handler(batch, None) == {"batchItemFailures": []}

    kinds = [alert.kind for alert in wired.sent]
    assert kinds.count(LOW_STOCK) == 1
    assert kinds.count(HIGH_REJECT_RATE) == 7

    # Every reject alert points to an ASSEMBLY-3 station in Monterrey: where the bad lot is installed
    for alert in wired.sent:
        if alert.kind == HIGH_REJECT_RATE:
            assert alert.metrics["plant"] == "PLANT-MTY"
            assert alert.metrics["station"].endswith("ASSEMBLY-3")


def test_replaying_the_session_sends_nothing_new(wired: CapturingNotifier, store: StateStore) -> None:
    """A full Lambda retry (every batch delivered twice) must not change stock or repeat alerts."""
    events = build_live_events(history_days=7, end_day=END_DAY, seed=42)
    for batch in kinesis_batches(events):
        handler_module.handler(batch, None)
    alerts_first_pass = len(wired.sent)
    stock_first_pass = store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"]

    for batch in kinesis_batches(events):
        handler_module.handler(batch, None)

    assert len(wired.sent) == alerts_first_pass
    assert store.part_stock("PLANT-MTY#CONNECTOR-C4")["on_hand"] == stock_first_pass


def test_transient_failure_is_reported_for_retry(monkeypatch: pytest.MonkeyPatch, wired: CapturingNotifier) -> None:
    """A throttling-type error must come back in batchItemFailures, never be swallowed."""

    def boom(record, deps):
        raise RuntimeError("ProvisionedThroughputExceeded")

    monkeypatch.setattr(handler_module, "process_record", boom)
    batch = kinesis_batches([{"event_id": "x"}])[0]
    assert handler_module.handler(batch, None) == {"batchItemFailures": [{"itemIdentifier": "0"}]}


def test_corrupted_scan_is_skipped_not_retried(wired: CapturingNotifier) -> None:
    """Bad data is logged and skipped: retrying it would block the shard forever."""
    corrupted = {"event_id": "EV-BAD-X", "event_type": "CONSUMED", "plant": None, "quantity": -5}
    batch = kinesis_batches([corrupted])[0]
    assert handler_module.handler(batch, None) == {"batchItemFailures": []}
