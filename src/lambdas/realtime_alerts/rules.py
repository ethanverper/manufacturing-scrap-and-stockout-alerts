"""Business rules of the real-time path: the two KPIs and the decision to alert.

Where it sits in the pipeline:
    After the Lambda has updated DynamoDB (level 3), it reads the part's stock and the station's
    hourly counters and asks this module: "should someone be alerted?". Everything here is pure math
    on numbers passed in, with no AWS calls, so every rule is unit tested.

The two alerts (thresholds in config/settings.yaml, section "alerts"):
    LOW_STOCK         stock coverage below N days -> Purchasing must expedite an order
    HIGH_REJECT_RATE  a station's reject rate in the current hour is statistically unusual
                      (above its p-chart control limit, the floor %, and with enough rejects)
                      -> Quality must inspect the station or quarantine the lot
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from src.common import config

# Environment variable names. Terraform sets them from config/settings.yaml (section "alerts"),
# so the Lambda in AWS uses exactly the values in the YAML.
ENV_REJECT_RATE_BASELINE_PCT = "REJECT_RATE_BASELINE_PCT"
ENV_REJECT_RATE_SIGMA = "REJECT_RATE_SIGMA"
ENV_REJECT_RATE_FLOOR_PCT = "REJECT_RATE_FLOOR_PCT"
ENV_REJECT_RATE_MIN_REJECTS = "REJECT_RATE_MIN_REJECTS"
ENV_STOCK_COVERAGE_THRESHOLD_DAYS = "STOCK_COVERAGE_THRESHOLD_DAYS"

# Alert kinds: also part of the de-duplication key and the email subject
LOW_STOCK = "LOW_STOCK"
HIGH_REJECT_RATE = "HIGH_REJECT_RATE"


@dataclass(frozen=True)
class AlertThresholds:
    """The numbers that decide when to alert (one field per key in config/settings.yaml "alerts")."""

    reject_rate_baseline_pct: float
    reject_rate_sigma: float
    reject_rate_floor_pct: float
    reject_rate_min_rejects: int
    stock_coverage_threshold_days: float

    @classmethod
    def from_env(cls) -> AlertThresholds:
        """Read the thresholds from the Lambda's environment variables.

        Raises:
            KeyError: If a variable is missing. Alerting with an invented default would be worse
                than failing at start-up, where the misconfiguration is obvious.
        """
        return cls(
            reject_rate_baseline_pct=float(config.get_str(ENV_REJECT_RATE_BASELINE_PCT)),
            reject_rate_sigma=float(config.get_str(ENV_REJECT_RATE_SIGMA)),
            reject_rate_floor_pct=float(config.get_str(ENV_REJECT_RATE_FLOOR_PCT)),
            reject_rate_min_rejects=int(config.get_str(ENV_REJECT_RATE_MIN_REJECTS)),
            stock_coverage_threshold_days=float(config.get_str(ENV_STOCK_COVERAGE_THRESHOLD_DAYS)),
        )


@dataclass(frozen=True)
class Alert:
    """An alert ready to be sent.

    Attributes:
        kind: LOW_STOCK or HIGH_REJECT_RATE.
        dedupe_key: Identifies "the same problem". Only one alert is sent per key: per part and day
            for low stock, per station and hour for rejects.
        subject: Email subject line (SNS limits subjects to 100 characters).
        message: Email body: what happened, the numbers, and the suggested action.
        metrics: The numbers behind the alert, for structured logs.
    """

    kind: str
    dedupe_key: str
    subject: str
    message: str
    metrics: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# KPI math
# ---------------------------------------------------------------------------


def stock_coverage_days(on_hand: float, planned_daily_usage: float) -> float:
    """Days of production the parts on the shelf can sustain at the planned rate.

    Args:
        on_hand: Units currently on the shelf (can be negative if the stock was over-consumed).
        planned_daily_usage: Units the plant plans to use per day.

    Returns:
        on_hand / planned_daily_usage.

    Raises:
        ValueError: If planned usage is not positive (a master-data error: coverage would be meaningless).
    """
    if planned_daily_usage <= 0:
        raise ValueError(f"planned_daily_usage must be positive, got {planned_daily_usage}")
    return on_hand / planned_daily_usage


def reject_rate_pct(consumed_units: int, rejected_units: int) -> float | None:
    """Percentage of parts that failed inspection: rejected / (consumed + rejected) x 100.

    Returns:
        The rate, or None when no parts were handled (0 / 0 is "no data", not "0%").
    """
    handled = consumed_units + rejected_units
    if handled == 0:
        return None
    return 100.0 * rejected_units / handled


def control_limit_pct(units_handled: int, baseline_pct: float, sigma: float) -> float:
    """Upper control limit of a p-chart: the highest reject rate that is still "normal" for this volume.

    Statistical process control (the standard tool in manufacturing quality): with a normal reject
    rate p and n parts handled, the observed rate varies around p by sqrt(p(1-p)/n). Rates more than
    `sigma` of those deviations above p are unlikely to be chance, so they signal a real problem.

    Example with p = 1.2%: 60 parts -> limit 5.4%; 200 parts -> limit 3.5%.

    Args:
        units_handled: Parts the station handled in the hour (consumed + rejected), > 0.
        baseline_pct: Normal reject rate, in percent.
        sigma: Width of the band, in standard deviations (3 is the industry convention).

    Returns:
        The control limit, in percent.
    """
    # Work in proportions (0-1) for the formula, then convert back to percent
    p = baseline_pct / 100.0
    standard_error = math.sqrt(p * (1.0 - p) / units_handled)
    return 100.0 * (p + sigma * standard_error)


# ---------------------------------------------------------------------------
# Alert decisions
# ---------------------------------------------------------------------------


def check_low_stock(
    *,
    plant: str,
    part: str,
    supplier: str,
    on_hand: float,
    planned_daily_usage: float,
    day: str,
    thresholds: AlertThresholds,
) -> Alert | None:
    """Return a LOW_STOCK alert if the part's coverage is below the threshold, otherwise None.

    Args:
        plant, part, supplier: What is running out, and who to call.
        on_hand: Units on the shelf after the latest movement.
        planned_daily_usage: Planned units per day for this part in this plant.
        day: UTC date "YYYY-MM-DD". Part of the de-duplication key: at most one alert per part per day.
        thresholds: The configured thresholds.
    """
    coverage = stock_coverage_days(on_hand, planned_daily_usage)

    # Above the threshold: nothing to say
    if coverage >= thresholds.stock_coverage_threshold_days:
        return None

    # Hours left are easier to act on than "0.94 days"
    hours_left = max(coverage, 0.0) * 24
    return Alert(
        kind=LOW_STOCK,
        dedupe_key=f"{LOW_STOCK}#{plant}#{part}#{day}",
        subject=f"[{plant}] Low stock: {part} covers {coverage:.2f} days",
        message=(
            f"{part} at {plant} has {on_hand:,.0f} units on hand: {coverage:.2f} days "
            f"(~{hours_left:.0f} hours) of planned usage ({planned_daily_usage:,.0f} units/day). "
            f"Threshold: {thresholds.stock_coverage_threshold_days} day(s).\n\n"
            f"Action: Purchasing should expedite the next delivery from {supplier} "
            f"before the line stops."
        ),
        metrics={
            "plant": plant,
            "part": part,
            "supplier": supplier,
            "on_hand": on_hand,
            "planned_daily_usage": planned_daily_usage,
            "coverage_days": round(coverage, 3),
        },
    )


def check_reject_rate(
    *,
    plant: str,
    station: str,
    hour_bucket: str,
    consumed_units: int,
    rejected_units: int,
    thresholds: AlertThresholds,
) -> Alert | None:
    """Return a HIGH_REJECT_RATE alert if the station's hourly reject rate is unusual, otherwise None.

    Three conditions, all required:
      1. at least `reject_rate_min_rejects` rejected units (one or two rejects are noise);
      2. the rate is above the p-chart control limit for the volume handled (see control_limit_pct);
      3. the rate is above the floor (never alert on tiny rates, however stable the station is).

    Args:
        plant, station: Where it is happening.
        hour_bucket: UTC hour "YYYY-MM-DDTHH" of the counters. One alert per station per hour.
        consumed_units, rejected_units: The station's counters for that hour (all parts).
        thresholds: The configured thresholds.
    """
    # 1. Too few rejects to mean anything yet
    if rejected_units < thresholds.reject_rate_min_rejects:
        return None

    # rejected_units > 0 here, so the rate is defined
    handled = consumed_units + rejected_units
    rate = rejected_units * 100.0 / handled

    # 2 and 3. The limit is the stricter of the statistical limit and the floor
    limit = max(
        control_limit_pct(handled, thresholds.reject_rate_baseline_pct, thresholds.reject_rate_sigma),
        thresholds.reject_rate_floor_pct,
    )
    if rate <= limit:
        return None

    return Alert(
        kind=HIGH_REJECT_RATE,
        dedupe_key=f"{HIGH_REJECT_RATE}#{plant}#{station}#{hour_bucket}",
        subject=f"[{plant}] {station} rejecting {rate:.1f}% of parts",
        message=(
            f"Station {station} at {plant} rejected {rejected_units} of {handled} parts "
            f"({rate:.1f}%) in the hour starting {hour_bucket}:00 UTC. "
            f"Normal is {thresholds.reject_rate_baseline_pct}%; the control limit for {handled} parts "
            f"is {limit:.1f}%.\n\n"
            f"Action: Quality should inspect the station. If the rejects are SUPPLIER_DEFECT and "
            f"concentrated in one lot, quarantine that lot."
        ),
        metrics={
            "plant": plant,
            "station": station,
            "hour_bucket": hour_bucket,
            "consumed_units": consumed_units,
            "rejected_units": rejected_units,
            "reject_rate_pct": round(rate, 2),
            "control_limit_pct": round(limit, 2),
        },
    )
