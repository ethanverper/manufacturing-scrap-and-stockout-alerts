"""The real-time business rules: KPI math and the decision to alert."""

from __future__ import annotations

import pytest

from src.common.settings import settings
from src.lambdas.realtime_alerts import rules

# The same thresholds Terraform will pass to the Lambda (config/settings.yaml, section "alerts")
THRESHOLDS = rules.AlertThresholds(**settings()["alerts"])


def test_stock_coverage_is_on_hand_over_planned_usage() -> None:
    """5,760 connectors a day; 6,059 on hand -> 1.05 days."""
    assert rules.stock_coverage_days(6059, 5760) == pytest.approx(1.052, abs=0.001)


def test_stock_coverage_needs_positive_usage() -> None:
    """Zero planned usage is a master-data error, not infinite coverage."""
    with pytest.raises(ValueError):
        rules.stock_coverage_days(100, 0)


def test_reject_rate_handles_no_data() -> None:
    """No parts handled -> None (unknown), not 0%."""
    assert rules.reject_rate_pct(0, 0) is None
    assert rules.reject_rate_pct(95, 5) == 5.0


def test_low_stock_alert_fires_below_one_day() -> None:
    """5,757 connectors (the live-session crossing) -> alert, with a dedupe key per part and day."""
    alert = rules.check_low_stock(
        plant="PLANT-MTY",
        part="CONNECTOR-C4",
        supplier="SUP-NORDCAB",
        on_hand=5757,
        planned_daily_usage=5760,
        day="2026-10-28",
        thresholds=THRESHOLDS,
    )
    assert alert is not None
    assert alert.kind == rules.LOW_STOCK
    assert alert.dedupe_key == "LOW_STOCK#PLANT-MTY#CONNECTOR-C4#2026-10-28"
    assert "SUP-NORDCAB" in alert.message
    assert len(alert.subject) <= 100  # SNS subject limit


def test_no_low_stock_alert_at_or_above_threshold() -> None:
    """Exactly 1.0 day is not below the threshold."""
    assert (
        rules.check_low_stock(
            plant="PLANT-MTY",
            part="CONNECTOR-C4",
            supplier="SUP-NORDCAB",
            on_hand=5760,
            planned_daily_usage=5760,
            day="2026-10-28",
            thresholds=THRESHOLDS,
        )
        is None
    )


def test_control_limit_adapts_to_volume() -> None:
    """p-chart with a 1.2% baseline: few parts need a higher rate before it is unusual."""
    assert rules.control_limit_pct(60, 1.2, 3.0) == pytest.approx(5.41, abs=0.01)
    assert rules.control_limit_pct(200, 1.2, 3.0) == pytest.approx(3.51, abs=0.01)


@pytest.mark.parametrize(
    ("consumed", "rejected", "fires"),
    [
        (56, 4, True),  # 6.7% of 60 parts: above the 5.4% limit (the bad-lot station)
        (57, 3, False),  # only 3 rejects: below the minimum, treated as noise
        (190, 10, True),  # 5.0% of 200 parts: above the 3.5% limit
        (194, 6, False),  # 3.0% of 200 parts: below the 3.5% limit
        (31, 1, False),  # 1 of 32 (3.1%): the false alarm the old fixed-3% rule raised
    ],
)
def test_reject_rate_alert_uses_control_limit_and_minimum_rejects(consumed: int, rejected: int, fires: bool) -> None:
    """Alert only when the hour is statistically unusual AND has enough rejects."""
    alert = rules.check_reject_rate(
        plant="PLANT-MTY",
        station="LINE-2-ASSEMBLY-3",
        hour_bucket="2026-10-28T13",
        consumed_units=consumed,
        rejected_units=rejected,
        thresholds=THRESHOLDS,
    )
    assert (alert is not None) == fires
    if alert is not None:
        assert alert.dedupe_key == "HIGH_REJECT_RATE#PLANT-MTY#LINE-2-ASSEMBLY-3#2026-10-28T13"
        assert alert.metrics["control_limit_pct"] >= 3.0


def test_thresholds_from_env_fail_fast_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Lambda deployed without its thresholds must refuse to run instead of guessing."""
    for name in (
        rules.ENV_REJECT_RATE_BASELINE_PCT,
        rules.ENV_REJECT_RATE_SIGMA,
        rules.ENV_REJECT_RATE_FLOOR_PCT,
        rules.ENV_REJECT_RATE_MIN_REJECTS,
        rules.ENV_STOCK_COVERAGE_THRESHOLD_DAYS,
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(KeyError):
        rules.AlertThresholds.from_env()


def test_thresholds_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment values (text) become numbers."""
    monkeypatch.setenv(rules.ENV_REJECT_RATE_BASELINE_PCT, "1.2")
    monkeypatch.setenv(rules.ENV_REJECT_RATE_SIGMA, "3.0")
    monkeypatch.setenv(rules.ENV_REJECT_RATE_FLOOR_PCT, "3.0")
    monkeypatch.setenv(rules.ENV_REJECT_RATE_MIN_REJECTS, "4")
    monkeypatch.setenv(rules.ENV_STOCK_COVERAGE_THRESHOLD_DAYS, "1.0")
    assert rules.AlertThresholds.from_env() == THRESHOLDS
