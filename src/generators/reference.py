"""Master data for the simulated plants (what an ERP would hold).

Where it sits in the pipeline:
    This module is the single source of truth for plants, lines, stations, suppliers, parts,
    bills of materials and the production plan. The simulator uses it to create events, and
    `history.py` exports it as CSV files to `s3://<bucket>/reference/`, where the Glue job
    reads it to enrich events (unit cost, planned usage, planned units).

Where the values live:
    The numbers themselves are in config/settings.yaml (plant layout) and config/master_data.yaml
    (suppliers, parts, BOM). This module turns them into typed objects and derives the values that
    must agree with each other (planned usage, the hourly plan), so tests can check them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from src.common.settings import master_data, settings

# ---------------------------------------------------------------------------
# Plant layout (values come from config/settings.yaml, section "plant")
# ---------------------------------------------------------------------------

_PLANT = settings()["plant"]

# Two plants so the "Plant" level of every KPI hierarchy has something to compare
PLANTS: tuple[str, ...] = tuple(_PLANT["plants"])

# Every plant runs the same assembly lines
LINES: tuple[str, ...] = tuple(_PLANT["lines"])

# Station types on every line, in the order a unit travels through them
STATION_TYPES: tuple[str, ...] = tuple(_PLANT["station_types"])

# The line moves in batches of finished units, a fixed number of batches per hour
UNITS_PER_BATCH: int = _PLANT["units_per_batch"]
BATCHES_PER_HOUR: int = _PLANT["batches_per_hour"]

# The production plan: what each line is expected to produce every hour
PLANNED_UNITS_PER_HOUR: int = UNITS_PER_BATCH * BATCHES_PER_HOUR

# Lines run around the clock (three 8-hour shifts)
HOURS_PER_DAY: int = _PLANT["hours_per_day"]


# ---------------------------------------------------------------------------
# Suppliers and parts (values come from config/master_data.yaml)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Supplier:
    """A company that ships parts to the plants."""

    supplier_id: str
    name: str


@dataclass(frozen=True)
class Part:
    """A purchased part.

    Attributes:
        part_id: Code printed on the bin label, e.g. "BATTERY-A12".
        supplier_id: Who ships it (one supplier per part keeps the story readable).
        unit_cost: Purchase price in USD; turns rejected units into scrap cost.
        delivery_every_days: How often a truck arrives (1 = daily, 2 = every other day).
        target_coverage_days: Each delivery refills stock up to this many days of planned usage.
    """

    part_id: str
    supplier_id: str
    unit_cost: float
    delivery_every_days: int
    target_coverage_days: float


_MASTER = master_data()

# Supplier id -> Supplier
SUPPLIERS: dict[str, Supplier] = {
    supplier_id: Supplier(supplier_id, name) for supplier_id, name in _MASTER["suppliers"].items()
}

# Part id -> Part, with numbers converted to the right types
PARTS: dict[str, Part] = {
    part_id: Part(
        part_id=part_id,
        supplier_id=spec["supplier"],
        unit_cost=float(spec["unit_cost"]),
        delivery_every_days=int(spec["delivery_every_days"]),
        target_coverage_days=float(spec["target_coverage_days"]),
    )
    for part_id, spec in _MASTER["parts"].items()
}

# Bill of materials: parts each station type installs per finished unit
STATION_BOM: dict[str, dict[str, int]] = {
    station_type: dict(bom) for station_type, bom in _MASTER["station_bom"].items()
}


# ---------------------------------------------------------------------------
# Derived values
# ---------------------------------------------------------------------------


def station_id(line: str, station_type: str) -> str:
    """Build a station code that is unique inside a plant.

    Why: the same station type exists on every line, and alerts must point to one physical
    station ("LINE-2-ASSEMBLY-3"), not to "ASSEMBLY-3" somewhere in the plant.

    Args:
        line: Line code, e.g. "LINE-2".
        station_type: Station type, e.g. "ASSEMBLY-3".

    Returns:
        The station code, e.g. "LINE-2-ASSEMBLY-3".
    """
    return f"{line}-{station_type}"


def station_type_of_part(part_id: str) -> str:
    """Return the station type that installs a part.

    Args:
        part_id: Part code.

    Returns:
        The station type whose BOM contains the part.

    Raises:
        KeyError: If no station uses the part (a master-data error worth failing on).
    """
    # Each part is installed at exactly one station type, so the first match is the answer
    for station_type, bom in STATION_BOM.items():
        if part_id in bom:
            return station_type
    raise KeyError(f"Part {part_id} is not in any station BOM")


def planned_daily_usage(part_id: str) -> float:
    """Units of a part one plant plans to use per day.

    Formula: units per finished unit (BOM) x planned units per line per day x number of lines.
    This is the denominator of the stock coverage KPI.

    Args:
        part_id: Part code.

    Returns:
        Planned units per plant per day.
    """
    # How many of this part go into one finished unit
    per_unit = STATION_BOM[station_type_of_part(part_id)][part_id]

    # Every line builds the same product at the same planned rate, all day
    planned_units_per_day = PLANNED_UNITS_PER_HOUR * HOURS_PER_DAY * len(LINES)
    return float(per_unit * planned_units_per_day)


# ---------------------------------------------------------------------------
# Exports (rows written as CSV by history.py)
# ---------------------------------------------------------------------------


def parts_rows() -> list[dict]:
    """One row per plant and part: supplier, cost and planned usage."""
    rows = []
    for plant in PLANTS:
        for part in PARTS.values():
            rows.append(
                {
                    "plant": plant,
                    "part": part.part_id,
                    "supplier": part.supplier_id,
                    "supplier_name": SUPPLIERS[part.supplier_id].name,
                    "unit_cost": part.unit_cost,
                    "planned_daily_usage": planned_daily_usage(part.part_id),
                }
            )
    return rows


def stations_rows() -> list[dict]:
    """One row per physical station: plant, line, station and its type."""
    return [
        {"plant": plant, "line": line, "station": station_id(line, st), "station_type": st}
        for plant in PLANTS
        for line in LINES
        for st in STATION_TYPES
    ]


def plan_rows(days: list[date]) -> list[dict]:
    """The production plan: planned units per plant, line, date and local hour.

    Args:
        days: Calendar days to plan (history days plus the live-session day).

    Returns:
        One row per plant x line x day x hour.
    """
    return [
        {
            "plant": plant,
            "line": line,
            "plan_date": day.isoformat(),
            "local_hour": hour,
            "planned_units": PLANNED_UNITS_PER_HOUR,
        }
        for day in days
        for plant in PLANTS
        for line in LINES
        for hour in range(HOURS_PER_DAY)
    ]
