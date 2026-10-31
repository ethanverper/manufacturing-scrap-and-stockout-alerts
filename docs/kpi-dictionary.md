# KPI dictionary

Every KPI is computed in the Glue job at its **lowest grain** (gold tables) and rolled up to the upper levels with `GROUP BY ROLLUP` in Athena (`sql/`). The two real-time KPIs are also computed by the Lambda for alerting.

## 1. Reject rate %

| | |
|---|---|
| **Business definition** | Share of parts taken from stock that failed inspection |
| **Formula** | `rejected_units / (consumed_units + rejected_units) × 100` |
| **Lowest grain (gold)** | `dt · plant · line · station · part · supplier · lot`, with `consumed_units`, `rejected_units`, and rejects split into `supplier_defect_units` / `process_defect_units` (`gold/reject_rate`) |
| **Hierarchies** | Plant → Line → Station → Part (where it happens) · Supplier → Part → Lot (who causes it) |
| **Real-time version** | Same formula over the station's **current clock hour** (`station_hourly` table) |
| **Threshold** | Statistical process control (**p-chart**): alert when the station's hourly rate is above its upper control limit `UCL = p̄ + 3·√(p̄(1−p̄)/n)` (p̄ = 1.2% normal rate, n = parts handled), **and** above a 3% floor, **and** with ≥ 4 rejected units → email **Quality**. The limit adapts to volume: 5.4% for 60 parts/hour, 3.5% for 200. All values in `config/settings.yaml` → `alerts`. |
| **Owner** | Quality engineering |
| **Query** | `sql/01_reject_rate_by_location.sql` (where) · `sql/02_reject_rate_by_supplier_lot.sql` (who) |
| **Reading it** | A high rate concentrated in one **lot** with reason `SUPPLIER_DEFECT` → quarantine the lot and claim it from the supplier. A high rate spread across lots at one **station** with `PROCESS_DEFECT` → inspect the station. |

## 2. Stock coverage (days)

| | |
|---|---|
| **Business definition** | How many days the plant can keep producing with the parts on hand, at the planned usage rate |
| **Formula** | `units_on_hand / planned_daily_usage` |
| **Lowest grain (gold)** | `dt · plant · part · supplier` (end-of-day position, UTC day): `closing_units = opening stock + running sum of every movement` (`gold/stock_coverage`). Rebuilt from raw events independently of the Lambda, and reconciled with it in tests. |
| **Hierarchy** | Plant → Part → Supplier |
| **Real-time version** | `on_hand` from the `part_stock` table, after every movement |
| **Threshold** | **< 1.0 day** → email **Purchasing** (once per part per day) |
| **Owner** | Purchasing / material planning |
| **Query** | `sql/03_stock_coverage.sql` (no rollup: days of coverage are not additive) |
| **Reading it** | Under 1 day means the next delivery must arrive within 24 h or the line stops. The supplier column says who to call. |

## 3. Throughput vs plan %

| | |
|---|---|
| **Business definition** | Finished units produced compared with the production plan |
| **Formula** | `produced_units / planned_units × 100` |
| **Lowest grain (gold)** | `dt · plant · line · local_date · local_hour` (+ `shift`), with `planned_units` and `produced_units` (`gold/throughput`). Built from the plan, so an hour without production shows 0, but only for hours in which the plant sent data: a gap with no scans at all is "no data", not "zero produced" (lesson 7). |
| **Hierarchy** | Plant → Line → Shift → Hour |
| **Shifts** | Morning 06:00–14:00 · Evening 14:00–22:00 · Night 22:00–06:00 (plant local time) |
| **Threshold** | Report only. Below 90% for a full shift is highlighted in the README charts. |
| **Owner** | Plant manager |
| **Query** | `sql/04_throughput_vs_plan.sql` |
| **Reading it** | A line consistently behind on one shift points to staffing or process. Comparing it with the reject rate tells whether quality is the cause. |

## 4. Scrap cost ($)

| | |
|---|---|
| **Business definition** | Money lost to rejected parts |
| **Formula** | `rejected_units × unit_cost` (unit cost from `reference/parts.csv`) |
| **Lowest grain (gold)** | `dt · plant · line · station · part · supplier · reason_code` |
| **Hierarchies** | Plant → Supplier → Part (finance and chargebacks) · Plant → Line → Station (operations) |
| **Split** | `SUPPLIER_DEFECT` = **recoverable** (chargeback candidate) · `PROCESS_DEFECT` = **internal loss** |
| **Threshold** | Report only |
| **Owner** | Plant controller (finance) |
| **Query** | `sql/05_scrap_cost_by_supplier.sql` |
| **Reading it** | The recoverable amount per supplier is the claim to file. The internal loss per station is the business case for fixing it. |

## Shared definitions

| Term | Definition |
|---|---|
| `dt` | Event date in UTC (`event_time`), the partition of every raw, curated and gold table |
| Consumed units | Sum of `quantity` for `CONSUMED` events |
| Rejected units | Sum of `quantity` for `REJECTED` events |
| Produced units | Sum of `quantity` for `PRODUCED` events (end of line) |
| Duplicate | Two records with the same `event_id`. Only the first one counts, everywhere. |
| Invalid record | Missing field, unknown event type, unreadable time, quantity ≤ 0, a part code unknown in that plant (any event type), or an unknown station. Goes to `quarantine/` with a reason. |
| Rates in gold | Never stored. Gold tables keep additive counts; every rate is computed **after** rolling up (a plant's reject rate is not the average of its stations' rates). |
