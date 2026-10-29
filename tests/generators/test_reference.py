"""Master data must be internally consistent: the KPIs divide by these numbers."""

from __future__ import annotations

from datetime import date

from src.generators import reference


def test_every_part_is_installed_by_exactly_one_station_type() -> None:
    """A part without a station would never be consumed; one in two stations would double its usage."""
    for part in reference.PARTS:
        owners = [st for st, bom in reference.STATION_BOM.items() if part in bom]
        assert len(owners) == 1, part


def test_planned_daily_usage_follows_the_bom() -> None:
    """CONNECTOR-C4: 4 per unit x 20 units/hour x 24 hours x 3 lines = 5,760 per plant per day."""
    assert reference.planned_daily_usage("CONNECTOR-C4") == 5760.0


def test_plan_has_one_row_per_plant_line_day_hour() -> None:
    """2 plants x 3 lines x 2 days x 24 hours = 288 rows, each at the planned hourly rate."""
    rows = reference.plan_rows([date(2026, 10, 27), date(2026, 10, 28)])
    assert len(rows) == 288
    assert {row["planned_units"] for row in rows} == {reference.PLANNED_UNITS_PER_HOUR}


def test_station_ids_are_unique_within_a_plant() -> None:
    """Alerts point to one physical station, so codes must not repeat inside a plant."""
    rows = reference.stations_rows()
    keys = [(row["plant"], row["station"]) for row in rows]
    assert len(keys) == len(set(keys))
