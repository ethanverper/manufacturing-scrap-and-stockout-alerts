"""Hour-by-hour simulation of the two plants: deliveries, installs, rejects and finished units.

Where it sits in the pipeline:
    Both data producers use this simulator:
      - history.py runs it for 7 days and writes the events straight to S3 raw/ (backfill);
      - live.py runs the same 7 days in memory to reach the same stock and lots, then streams
        the next morning to Kinesis.
    Because the simulation is seeded, both runs produce the same plant state.

How the plant works in the simulation:
    - Every line builds batches of 5 units, 4 batches per hour, around the clock.
    - Each station installs its BOM parts (CONSUMED). Some parts fail inspection (REJECTED):
      supplier defects depend on the lot, process defects on the station.
    - Stock per plant and part is a FIFO queue of lots: the oldest lot is used first, which is
      how a bad lot reaches the stations a while after it is received.
    - At 07:00 local, due deliveries refill each part up to its target coverage (RECEIVED).
    - The end of each batch emits PRODUCED with the finished units.
"""

from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from src.generators import story
from src.generators.events import (
    CONSUMED,
    END_OF_LINE,
    PLANT_TZ,
    PROCESS_DEFECT,
    PRODUCED,
    RECEIVED,
    REJECTED,
    SUPPLIER_DEFECT,
    WAREHOUSE,
    make_event,
    make_event_id,
)
from src.generators.reference import (
    BATCHES_PER_HOUR,
    LINES,
    PARTS,
    PLANTS,
    STATION_BOM,
    STATION_TYPES,
    UNITS_PER_BATCH,
    planned_daily_usage,
    station_id,
)

# Trucks arrive once a day at 07:00 plant local time
DELIVERY_HOUR: int = 7

# Deliveries are rounded up to full pallets of 100 units
PALLET_SIZE: int = 100

# Lot numbers start here; each new delivery takes the next number
FIRST_LOT_NUMBER: int = 7000


@dataclass
class Lot:
    """A delivered lot of one part, and how much of it is still on the shelf."""

    lot_id: str
    supplier: str
    remaining: float


class PlantSimulator:
    """Simulates both plants hour by hour and keeps their stock state between calls."""

    def __init__(self, history_days: int, first_day: date, seed: int = 42) -> None:
        """Prepare the plants with their opening stock.

        Args:
            history_days: Length of the history. The incidents are placed relative to it.
            first_day: Calendar date (plant local) of simulated day 0.
            seed: Random seed. The same seed always produces the same events.
        """
        self.history_days = history_days
        self.first_day = first_day

        # One seeded generator drives every random decision, so runs are reproducible
        self.rng = random.Random(seed)

        # The three incidents planted in the data (see story.py)
        self.bad_lot, self.shortage, self.line_behind = story.build_incidents(history_days)

        # Counters behind unique event ids and lot numbers
        self._event_sequence = 0
        self._lot_number = FIRST_LOT_NUMBER

        # Stock: one FIFO queue of lots per (plant, part)
        self.lots: dict[tuple[str, str], deque[Lot]] = {}
        self._open_initial_stock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def calendar_day(self, day: int) -> date:
        """Plant-local calendar date of a simulated day index."""
        return self.first_day + timedelta(days=day)

    def on_hand(self, plant: str, part: str) -> float:
        """Units of a part currently on the shelf in a plant (all lots together)."""
        return sum(lot.remaining for lot in self.lots[(plant, part)])

    def simulate_day(self, day: int, hours: range = range(24)) -> list[dict]:
        """Simulate the given local hours of one day and return their events, in time order.

        Args:
            day: Simulated day index (0 = first history day).
            hours: Local hours to simulate. The live session uses only a few morning hours.

        Returns:
            Events of every simulated hour.
        """
        events: list[dict] = []
        for hour in hours:
            events.extend(self.simulate_hour(day, hour))
        return events

    def simulate_hour(self, day: int, hour: int) -> list[dict]:
        """Simulate one local hour in both plants.

        Args:
            day: Simulated day index.
            hour: Local hour (0-23).

        Returns:
            The hour's events, sorted by event time.
        """
        # Start of the hour in plant local time; every event of the hour is placed after it
        hour_start = datetime.combine(self.calendar_day(day), time(hour), tzinfo=PLANT_TZ)
        events: list[dict] = []

        # Morning deliveries come first, so the day's production can use them
        if hour == DELIVERY_HOUR:
            events.extend(self._receive_deliveries(day, hour_start))

        # The shift decides whether the line-behind-plan incident applies
        shift = story.shift_of(hour)

        # Run every planned batch of every line in both plants
        for plant in PLANTS:
            for line in LINES:
                for batch in range(BATCHES_PER_HOUR):
                    # Some night-shift batches on LINE-3 don't run (incident 3)
                    if not story.batch_runs(self.line_behind, line, shift, self.rng):
                        continue
                    batch_start = hour_start + timedelta(minutes=15 * batch)
                    events.extend(self._run_batch(day, plant, line, batch_start))

        # ISO UTC strings sort chronologically; Python's sort is stable for equal times
        events.sort(key=lambda event: event["event_time"])
        return events

    def stock_snapshot_rows(self) -> list[dict]:
        """Current stock per plant and part: the opening position for the real-time state.

        Returns:
            Rows with on-hand units, planned usage, coverage in days and the lot in use.
        """
        rows = []
        for (plant, part), queue in sorted(self.lots.items()):
            usage = planned_daily_usage(part)
            on_hand = self.on_hand(plant, part)
            rows.append(
                {
                    "plant": plant,
                    "part": part,
                    "supplier": PARTS[part].supplier_id,
                    "current_lot": queue[0].lot_id,
                    "on_hand": round(on_hand),
                    "planned_daily_usage": usage,
                    "coverage_days": round(on_hand / usage, 3),
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Stock and lots
    # ------------------------------------------------------------------

    def _open_initial_stock(self) -> None:
        """Fill every plant and part with one lot sized to its target coverage."""
        for plant in PLANTS:
            for part in PARTS.values():
                # Start where a normal delivery would leave the shelf: at target coverage
                quantity = math.ceil(planned_daily_usage(part.part_id) * part.target_coverage_days)
                lot = Lot(self._new_lot_id(), part.supplier_id, quantity)
                self.lots[(plant, part.part_id)] = deque([lot])

    def _new_lot_id(self) -> str:
        """Next sequential lot id, skipping the number reserved for the bad lot."""
        self._lot_number += 1

        # LOT-7781 belongs to the bad-lot incident; a normal delivery must never reuse it
        if f"LOT-{self._lot_number}" == self.bad_lot.lot_id:
            self._lot_number += 1
        return f"LOT-{self._lot_number}"

    def _take_from_stock(self, plant: str, part: str, quantity: float) -> None:
        """Remove units from the shelf, oldest lot first (FIFO).

        The last lot is never removed from the queue: if stock runs out it goes negative,
        which the KPIs and alerts then show as a real shortage instead of hiding it.
        """
        queue = self.lots[(plant, part)]
        while quantity > 0:
            head = queue[0]

            # Empty the oldest lot completely and move on to the next one, if there is one
            if head.remaining <= quantity and len(queue) > 1:
                quantity -= head.remaining
                queue.popleft()
                continue

            # Otherwise take the rest from this lot and stop
            head.remaining -= quantity
            quantity = 0

    # ------------------------------------------------------------------
    # Event producers
    # ------------------------------------------------------------------

    def _next_event_id(self, local_time: datetime, plant: str) -> str:
        """Unique id for the next event (the counter never resets)."""
        self._event_sequence += 1
        return make_event_id(local_time.date(), plant, self._event_sequence)

    def _receive_deliveries(self, day: int, hour_start: datetime) -> list[dict]:
        """Receive the deliveries due today in both plants.

        Each due delivery refills the part up to its target coverage, rounded up to full pallets.

        Returns:
            One RECEIVED event per delivered lot.
        """
        events = []
        for plant in PLANTS:
            for part in PARTS.values():
                # Only on the part's delivery days (every day, or every other day)
                if day % part.delivery_every_days != 0:
                    continue

                # Incident 2: the shortage part's truck doesn't arrive
                if story.delivery_is_missed(self.shortage, plant, part.part_id, day):
                    continue

                # Order what's missing to reach the target coverage, in full pallets
                target = planned_daily_usage(part.part_id) * part.target_coverage_days
                missing = target - self.on_hand(plant, part.part_id)
                quantity = math.ceil(missing / PALLET_SIZE) * PALLET_SIZE
                if quantity <= 0:
                    continue

                # Incident 1: this specific delivery is the bad lot
                is_bad_lot = (plant, part.part_id, day) == (
                    self.bad_lot.plant,
                    self.bad_lot.part,
                    self.bad_lot.received_on_day,
                )
                lot_id = self.bad_lot.lot_id if is_bad_lot else self._new_lot_id()

                # The new lot goes to the back of the FIFO queue: older stock is used first
                self.lots[(plant, part.part_id)].append(Lot(lot_id, part.supplier_id, quantity))

                # Trucks unload at a random minute within the delivery hour
                received_at = hour_start + timedelta(minutes=self.rng.randint(0, 59))
                events.append(
                    make_event(
                        event_id=self._next_event_id(received_at, plant),
                        local_time=received_at,
                        event_type=RECEIVED,
                        plant=plant,
                        station=WAREHOUSE,
                        part=part.part_id,
                        supplier=part.supplier_id,
                        lot=lot_id,
                        quantity=quantity,
                    )
                )
        return events

    def _run_batch(self, day: int, plant: str, line: str, batch_start: datetime) -> list[dict]:
        """Build one batch of 5 units: every station installs its parts, then units are finished.

        Returns:
            The batch's CONSUMED, REJECTED and PRODUCED events.
        """
        events = []

        # The unit visits the stations in order, about 3 minutes apart
        for position, station_type in enumerate(STATION_TYPES):
            scan_time = batch_start + timedelta(minutes=3 * position, seconds=self.rng.randint(0, 59))
            station = station_id(line, station_type)

            # Install every part this station's BOM calls for
            for part, units_per_product in STATION_BOM[station_type].items():
                events.extend(self._install_part(day, plant, line, station, part, units_per_product, scan_time))

        # Finished units leave the end of the line 12 minutes after the batch started
        finished_at = batch_start + timedelta(minutes=12)
        events.append(
            make_event(
                event_id=self._next_event_id(finished_at, plant),
                local_time=finished_at,
                event_type=PRODUCED,
                plant=plant,
                line=line,
                station=END_OF_LINE,
                quantity=UNITS_PER_BATCH,
            )
        )
        return events

    def _install_part(
        self,
        day: int,
        plant: str,
        line: str,
        station: str,
        part: str,
        units_per_product: int,
        scan_time: datetime,
    ) -> list[dict]:
        """One station installs one part for the batch, rejecting the defective units.

        Returns:
            A CONSUMED event plus up to two REJECTED events (one per reason code).
        """
        # Good units needed: BOM x batch size, raised by rework during the shortage incident
        multiplier = story.usage_multiplier(self.shortage, plant, part, day)
        good_units = round(units_per_product * UNITS_PER_BATCH * multiplier)

        # The station uses the oldest lot on the shelf; its defect rate depends on that lot
        head = self.lots[(plant, part)][0]
        supplier_rate = story.supplier_defect_rate(self.bad_lot, plant, part, head.lot_id)

        # Defective units are counted separately by cause: the supplier's or the station's
        supplier_rejects = self._count_defects(good_units, supplier_rate)
        process_rejects = self._count_defects(good_units, story.BASELINE_PROCESS_DEFECT_RATE)

        # Rejected units also leave the shelf: they were taken and then scrapped
        self._take_from_stock(plant, part, good_units + supplier_rejects + process_rejects)

        # Every scan event shares the same location and lot details
        common = {
            "plant": plant,
            "line": line,
            "station": station,
            "part": part,
            "supplier": head.supplier,
            "lot": head.lot_id,
        }
        events = [
            make_event(
                event_id=self._next_event_id(scan_time, plant),
                local_time=scan_time,
                event_type=CONSUMED,
                quantity=good_units,
                **common,
            )
        ]

        # One REJECTED event per cause that actually happened in this batch
        for reason, count in ((SUPPLIER_DEFECT, supplier_rejects), (PROCESS_DEFECT, process_rejects)):
            if count > 0:
                events.append(
                    make_event(
                        event_id=self._next_event_id(scan_time, plant),
                        local_time=scan_time + timedelta(seconds=30),  # inspection right after install
                        event_type=REJECTED,
                        quantity=count,
                        reason_code=reason,
                        **common,
                    )
                )
        return events

    def _count_defects(self, units: int, rate: float) -> int:
        """Count how many of `units` are defective when each one fails with probability `rate`."""
        # One independent draw per unit (a binomial draw); batches are small, so this stays fast
        return sum(1 for _ in range(units) if self.rng.random() < rate)


def add_scanner_noise(events: list[dict], rng: random.Random, duplicate_rate: float, invalid_rate: float) -> list[dict]:
    """Add the data problems real scanners produce, so the pipeline must handle them.

    - Duplicates: a scanner times out and re-sends the same event (same event_id).
    - Invalid records: corrupted scans (negative quantity, unknown part, missing plant).

    Why a separate random generator: the noise must not change the simulator's random sequence,
    otherwise history.py and live.py would end up with different plant states.

    Args:
        events: Clean events, in time order.
        rng: A random generator dedicated to noise.
        duplicate_rate: Probability that an event is re-sent.
        invalid_rate: Probability that a corrupted copy of an event is added.

    Returns:
        A new list with the noise inserted right after the original events.
    """
    noisy: list[dict] = []
    for event in events:
        noisy.append(event)

        # Retry after a network timeout: an identical copy, same event_id
        if rng.random() < duplicate_rate:
            noisy.append(dict(event))

        # A corrupted scan with its own id, so it never hides a real event
        if rng.random() < invalid_rate:
            noisy.append(_corrupt(event, rng))
    return noisy


def _corrupt(event: dict, rng: random.Random) -> dict:
    """Return a broken copy of an event with exactly one realistic problem."""
    bad = dict(event, event_id=f"{event['event_id']}-X")

    # Pick one kind of corruption: a sign error, a mistyped part code or a lost field
    problem = rng.choice(("negative_quantity", "unknown_part", "missing_plant"))
    if problem == "negative_quantity":
        bad["quantity"] = -abs(event["quantity"])
    elif problem == "unknown_part":
        bad["part"] = "UNKNOWN-PART"
    else:
        bad["plant"] = None
    return bad
