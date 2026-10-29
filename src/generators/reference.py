"""Master data for the simulated plants (what an ERP would hold).

Where it sits in the pipeline:
    This module is the single source of truth for plants, lines, stations, suppliers, parts,
    bills of materials and the production plan. The simulator uses it to create events, and
    `history.py` exports it as CSV files to `s3://<bucket>/reference/`, where the Glue job
    reads it to enrich events (unit cost, planned usage, planned units).

Why it is code and not hand-written CSVs:
    Planned usage, the plan and the BOM must agree with each other. Deriving everything from
    one set of constants guarantees that, and lets tests check the numbers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# ---------------------------------------------------------------------------
# Plant layout
# ---------------------------------------------------------------------------

# Two plants so the "Plant" level of every KPI hierarchy has something to compare
PLANTS: tuple[str, ...] = ("PLANT-MTY", "PLANT-SLP")

# Every plant runs the same three assembly lines
LINES: tuple[str, ...] = ("LINE-1", "LINE-2", "LINE-3")

# Station types on every line, in the order a unit travels through them
STATION_TYPES: tuple[str, ...] = ("ASSEMBLY-1", "ASSEMBLY-2", "ASSEMBLY-3")

# The line moves in batches: 5 finished units per batch, 4 batches per hour
UNITS_PER_BATCH: int = 5
BATCHES_PER_HOUR: int = 4

# The production plan: what each line is expected to produce every hour (20 units)
PLANNED_UNITS_PER_HOUR: int = UNITS_PER_BATCH * BATCHES_PER_HOUR

# Lines run around the clock (three 8-hour shifts)
HOURS_PER_DAY: int = 24


# ---------------------------------------------------------------------------
# Suppliers and parts
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


SUPPLIERS: dict[str, Supplier] = {
    "SUP-VOLTEX": Supplier("SUP-VOLTEX", "Voltex Energy"),  # batteries
    "SUP-NORDCAB": Supplier("SUP-NORDCAB", "Nordcab Wiring"),  # cables, harnesses, connectors
    "SUP-PRECISA": Supplier("SUP-PRECISA", "Precisa Motion"),  # motors, controllers, sensors
    "SUP-ANDINA": Supplier("SUP-ANDINA", "Andina Metals"),  # housings, brackets, seals, screws
}

PARTS: dict[str, Part] = {
    # Expensive, slow-moving parts: delivered every other day, larger safety buffer
    "BATTERY-A12": Part("BATTERY-A12", "SUP-VOLTEX", 185.00, 2, 4.0),
    "MOTOR-M7": Part("MOTOR-M7", "SUP-PRECISA", 310.00, 2, 4.0),
    "CONTROLLER-K1": Part("CONTROLLER-K1", "SUP-PRECISA", 155.00, 2, 4.0),
    # Wiring and electronics: delivered daily
    "HARNESS-H2": Part("HARNESS-H2", "SUP-NORDCAB", 42.00, 1, 3.0),
    "CONNECTOR-C4": Part("CONNECTOR-C4", "SUP-NORDCAB", 3.50, 1, 3.0),
    "CABLE-C9": Part("CABLE-C9", "SUP-NORDCAB", 6.00, 1, 3.0),
    "SENSOR-S3": Part("SENSOR-S3", "SUP-PRECISA", 28.00, 1, 3.0),
    # Cheap mechanical parts: delivered daily
    "HOUSING-X1": Part("HOUSING-X1", "SUP-ANDINA", 22.00, 1, 3.0),
    "BRACKET-B5": Part("BRACKET-B5", "SUP-ANDINA", 4.00, 1, 3.0),
    "SEAL-S2": Part("SEAL-S2", "SUP-ANDINA", 1.20, 1, 3.0),
    "SCREW-KIT-K8": Part("SCREW-KIT-K8", "SUP-ANDINA", 0.80, 1, 3.0),
}

# Bill of materials: parts each station type installs per finished unit
STATION_BOM: dict[str, dict[str, int]] = {
    "ASSEMBLY-1": {"HOUSING-X1": 1, "BRACKET-B5": 2, "SEAL-S2": 2, "SCREW-KIT-K8": 1},
    "ASSEMBLY-2": {"HARNESS-H2": 1, "CONNECTOR-C4": 4, "CABLE-C9": 3, "SENSOR-S3": 2},
    "ASSEMBLY-3": {"BATTERY-A12": 1, "MOTOR-M7": 1, "CONTROLLER-K1": 1},
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
