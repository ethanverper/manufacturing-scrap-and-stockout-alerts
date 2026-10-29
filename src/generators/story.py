"""The incidents hidden in the simulated data (see docs/spec.md, "The story built into the data").

Why this module exists:
    A demo with perfectly random data shows nothing. These three incidents give the
    pipeline something real to find, and each one is aimed at a different KPI:

      1. Bad supplier lot      -> reject rate and scrap cost (SUPPLIER_DEFECT, chargeback)
      2. Part shortage         -> stock coverage below 1 day (real-time alert to Purchasing)
      3. Line behind plan      -> throughput vs plan on the night shift (process issue)

    Keeping every incident rule in one module means the rest of the simulator stays generic,
    and anyone can read exactly what was planted in the data.

Day indexes:
    Simulated days are numbered from 0 (first history day). With 7 history days, days 0-6 are
    history and day 7 is the live session streamed to Kinesis.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from src.common.settings import settings

# ---------------------------------------------------------------------------
# Normal plant behavior (the baseline every incident is compared against)
# ---------------------------------------------------------------------------

# All values come from config/settings.yaml (sections "simulation" and "plant")
_SIMULATION = settings()["simulation"]

# Share of parts that arrive defective from any supplier on a normal day
BASELINE_SUPPLIER_DEFECT_RATE: float = _SIMULATION["baseline_supplier_defect_rate"]

# Share of parts a station damages on a normal day (handling, wrong torque, etc.)
BASELINE_PROCESS_DEFECT_RATE: float = _SIMULATION["baseline_process_defect_rate"]

# Plant shifts by local start hour: morning, evening, and night (crossing midnight)
SHIFT_START_HOURS: dict[str, int] = dict(settings()["plant"]["shift_start_hours"])


# ---------------------------------------------------------------------------
# Incident definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BadLot:
    """A supplier lot with a manufacturing defect."""

    plant: str
    part: str
    lot_id: str
    received_on_day: int  # the delivery that brings the bad lot into the plant
    supplier_defect_rate: float  # replaces the baseline while this lot is in use


@dataclass(frozen=True)
class Shortage:
    """A part consumed faster than planned while its delivery is late."""

    plant: str
    part: str
    starts_on_day: int  # first day of extra usage and missed deliveries
    usage_multiplier: float  # rework makes stations use more parts than the BOM says


@dataclass(frozen=True)
class LineBehind:
    """A line that loses capacity on one shift (staffing or process issue)."""

    line: str
    shift: str
    lost_capacity: float  # share of batches that don't run on that shift


def build_incidents(history_days: int) -> tuple[BadLot, Shortage, LineBehind]:
    """Create the three incidents, placed relative to the length of the history.

    Why a function and not constants: the incidents must land near the end of the history and
    carry into the live session, wherever the history ends.

    Args:
        history_days: Number of history days (the live session is day `history_days`).

    Returns:
        (bad_lot, shortage, line_behind).
    """
    # The incident definitions (plants, parts, rates) are in config/settings.yaml
    incidents = _SIMULATION["incidents"]

    # 1. A bad supplier lot arrives a few days before the end of the history. Older stock is used
    #    first (FIFO), so it reaches the stations later and is still in use in the live session.
    lot = incidents["bad_lot"]
    bad_lot = BadLot(
        plant=lot["plant"],
        part=lot["part"],
        lot_id=lot["lot_id"],
        received_on_day=max(history_days - lot["received_days_before_end"], 0),
        supplier_defect_rate=lot["supplier_defect_rate"],
    )

    # 2. A part's deliveries stop and rework raises its usage, so coverage starts the live session
    #    just above 1 day and falls below it
    short = incidents["shortage"]
    shortage = Shortage(
        plant=short["plant"],
        part=short["part"],
        starts_on_day=history_days - short["starts_days_before_end"],
        usage_multiplier=short["usage_multiplier"],
    )

    # 3. One line loses part of its batches on one shift, in both plants
    behind = incidents["line_behind"]
    line_behind = LineBehind(line=behind["line"], shift=behind["shift"], lost_capacity=behind["lost_capacity"])

    return bad_lot, shortage, line_behind


# ---------------------------------------------------------------------------
# Rules the simulator asks about (pure functions: easy to test)
# ---------------------------------------------------------------------------


def shift_of(local_hour: int) -> str:
    """Return the shift that is working at a local hour.

    Args:
        local_hour: Hour of the day in plant local time (0-23).

    Returns:
        "MORNING", "EVENING" or "NIGHT".
    """
    # Morning and evening are simple ranges; everything else crosses midnight (night)
    if SHIFT_START_HOURS["MORNING"] <= local_hour < SHIFT_START_HOURS["EVENING"]:
        return "MORNING"
    if SHIFT_START_HOURS["EVENING"] <= local_hour < SHIFT_START_HOURS["NIGHT"]:
        return "EVENING"
    return "NIGHT"


def supplier_defect_rate(bad_lot: BadLot, plant: str, part: str, lot_id: str) -> float:
    """Probability that a part from a given lot arrives defective.

    Args:
        bad_lot: The bad-lot incident.
        plant, part, lot_id: Where and which lot is being installed.

    Returns:
        The bad lot's defect rate if it is that lot, otherwise the baseline.
    """
    # Only the exact plant/part/lot combination is defective; the same supplier's other lots are fine
    is_bad_lot = (plant, part, lot_id) == (bad_lot.plant, bad_lot.part, bad_lot.lot_id)
    return bad_lot.supplier_defect_rate if is_bad_lot else BASELINE_SUPPLIER_DEFECT_RATE


def usage_multiplier(shortage: Shortage, plant: str, part: str, day: int) -> float:
    """How much more of a part stations use than the BOM says (1.0 = exactly the BOM)."""
    # Extra usage only for the shortage part, at its plant, from the incident day on
    affected = (plant, part) == (shortage.plant, shortage.part) and day >= shortage.starts_on_day
    return shortage.usage_multiplier if affected else 1.0


def delivery_is_missed(shortage: Shortage, plant: str, part: str, day: int) -> bool:
    """True when a scheduled delivery does not arrive (the supplier's truck is late)."""
    return (plant, part) == (shortage.plant, shortage.part) and day >= shortage.starts_on_day


def batch_runs(line_behind: LineBehind, line: str, shift: str, rng: random.Random) -> bool:
    """Decide whether a planned batch actually runs.

    Args:
        line_behind: The line-behind-plan incident.
        line, shift: Where and when the batch is planned.
        rng: Seeded random generator (keeps the data reproducible).

    Returns:
        False for the share of batches the affected line loses on the affected shift.
    """
    # Every other line and shift runs every planned batch
    if (line, shift) != (line_behind.line, line_behind.shift):
        return True

    # The affected line/shift skips a batch with probability `lost_capacity`
    return rng.random() >= line_behind.lost_capacity
