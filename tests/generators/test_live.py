"""The live producer: Kinesis record format, batching limits and partial-failure retries."""

from __future__ import annotations

import json
from datetime import date

import pytest

from src.generators import live


def test_live_session_continues_the_story() -> None:
    """The morning session must reach the stream with the shortage still running."""
    events = live.build_live_events(history_days=7, end_day=date(2026, 10, 27), seed=42)
    assert len(events) > 500
    assert any(e["part"] == "CONNECTOR-C4" and e["plant"] == "PLANT-MTY" for e in events)


def test_kinesis_record_has_newline_and_part_partition_key() -> None:
    """The newline keeps one event per line in S3; the key keeps a part's movements in order."""
    record = live.to_kinesis_record({"plant": "PLANT-MTY", "part": "BATTERY-A12", "line": "LINE-1"})
    assert record["Data"].endswith(b"\n")
    assert json.loads(record["Data"])["part"] == "BATTERY-A12"
    assert record["PartitionKey"] == "PLANT-MTY#BATTERY-A12"


def test_batches_respect_the_put_records_limits() -> None:
    """1,201 records -> 500 + 500 + 201."""
    records = [{"Data": b"x" * 10, "PartitionKey": f"P#{i}"} for i in range(1201)]
    assert [len(batch) for batch in live.batches(records)] == [500, 500, 201]


class FlakyKinesis:
    """Fake Kinesis: the first call throttles the last record, the second accepts everything."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    def put_records(self, StreamName: str, Records: list[dict]) -> dict:  # noqa: N803 (boto3 argument names)
        self.calls.append(len(Records))
        if len(self.calls) == 1:
            results = [{}] * (len(Records) - 1) + [{"ErrorCode": "ProvisionedThroughputExceededException"}]
            return {"FailedRecordCount": 1, "Records": results}
        return {"FailedRecordCount": 0, "Records": [{}] * len(Records)}


def test_only_failed_records_are_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-sending the whole batch would duplicate the records that already succeeded."""
    monkeypatch.setattr(live.time, "sleep", lambda seconds: None)
    client = FlakyKinesis()
    records = [{"Data": b"a", "PartitionKey": "k1"}, {"Data": b"b", "PartitionKey": "k2"}]

    assert live.put_with_backoff(client, "stream", records) == 0
    assert client.calls == [2, 1]  # second call carries only the throttled record
