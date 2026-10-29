"""The incident rules decide what the demo finds, so each one is pinned down here."""

from __future__ import annotations

import random

from src.generators import story


def test_shifts_cover_the_day() -> None:
    """06-14 morning, 14-22 evening, the rest night (crossing midnight)."""
    assert story.shift_of(6) == "MORNING"
    assert story.shift_of(14) == "EVENING"
    assert story.shift_of(23) == "NIGHT"
    assert story.shift_of(3) == "NIGHT"


def test_only_the_bad_lot_has_the_high_defect_rate() -> None:
    """Same supplier and part, different lot -> baseline; the bad lot -> its own rate."""
    bad_lot, _, _ = story.build_incidents(history_days=7)
    assert story.supplier_defect_rate(bad_lot, "PLANT-MTY", "BATTERY-A12", "LOT-7781") == 0.15
    assert (
        story.supplier_defect_rate(bad_lot, "PLANT-MTY", "BATTERY-A12", "LOT-7001")
        == story.BASELINE_SUPPLIER_DEFECT_RATE
    )
    assert (
        story.supplier_defect_rate(bad_lot, "PLANT-SLP", "BATTERY-A12", "LOT-7781")
        == story.BASELINE_SUPPLIER_DEFECT_RATE
    )


def test_shortage_starts_on_the_last_history_day() -> None:
    """Extra usage and missed deliveries begin on day 6 of 7 and continue into the live day 7."""
    _, shortage, _ = story.build_incidents(history_days=7)
    assert story.usage_multiplier(shortage, "PLANT-MTY", "CONNECTOR-C4", 5) == 1.0
    assert story.usage_multiplier(shortage, "PLANT-MTY", "CONNECTOR-C4", 6) == 1.25
    assert story.delivery_is_missed(shortage, "PLANT-MTY", "CONNECTOR-C4", 7)
    assert not story.delivery_is_missed(shortage, "PLANT-SLP", "CONNECTOR-C4", 7)


def test_only_line_3_night_loses_batches() -> None:
    """Over many draws, LINE-3 night runs ~88% of batches; any other line/shift runs all of them."""
    _, _, line_behind = story.build_incidents(history_days=7)
    rng = random.Random(0)
    runs = sum(story.batch_runs(line_behind, "LINE-3", "NIGHT", rng) for _ in range(10_000))
    assert 0.86 < runs / 10_000 < 0.90
    assert all(story.batch_runs(line_behind, "LINE-1", "NIGHT", rng) for _ in range(100))
