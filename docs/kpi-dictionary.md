# KPI dictionary

Every KPI is computed in the Glue job at its **lowest grain** (gold tables) and rolled up to the upper levels with `GROUP BY ROLLUP` in Athena (`sql/`). The two real-time KPIs are also computed by the Lambda for alerting.

## 1. Reject rate %

| | |
|---|---|
| **Business definition** | Share of parts taken from stock that failed inspection |
| **Formula** | `rejected_units / (consumed_units + rejected_units) × 100` |
| **Lowest grain (gold)** | `dt · plant · line · station · part · supplier · lot · reason_code` |
| **Hierarchies** | Plant → Line → Station → Part (where it happens) · Supplier → Part → Lot (who causes it) |
| **Real-time version** | Same formula over the station's **current clock hour** (`station_hourly` table) |
| **Threshold** | Statistical process control (**p-chart**): alert when the station's hourly rate is above its upper control limit `UCL = p̄ + 3·√(p̄(1−p̄)/n)` (p̄ = 1.2% normal rate, n = parts handled), **and** above a 3% floor, **and** with ≥ 4 rejected units → email **Quality**. The limit adapts to volume: 5.4% for 60 parts/hour, 3.5% for 200. All values in `config/settings.yaml` → `alerts`. |
| **Owner** | Quality engineering |
| **Query** | `sql/01_reject_rate_rollup.sql` |
| **Reading it** | A high rate concentrated in one **lot** with reason `SUPPLIER_DEFECT` → quarantine the lot and claim it from the supplier. A high rate spread across lots at one **station** with `PROCESS_DEFECT` → inspect the station. |

## 2. Stock coverage (days)

| | |
|---|---|
| **Business definition** | How many days the plant can keep producing with the parts on hand, at the planned usage rate |
| **Formula** | `units_on_hand / planned_daily_usage` |
| **Lowest grain (gold)** | `dt · plant · part · supplier` (end-of-day position) |
| **Hierarchy** | Plant → Part → Supplier |
| **Real-time version** | `on_hand` from the `part_stock` table, after every movement |
| **Threshold** | **< 1.0 day** → email **Purchasing** (once per part per day) |
| **Owner** | Purchasing / material planning |
| **Query** | `sql/02_stock_coverage.sql` |
| **Reading it** | Under 1 day means the next delivery must arrive within 24 h or the line stops. The supplier column says who to call. |

## 3. Throughput vs plan %

| | |
|---|---|
| **Business definition** | Finished units produced compared with the production plan |
| **Formula** | `produced_units / planned_units × 100` |
| **Lowest grain (gold)** | `dt · plant · line · shift · hour` |
| **Hierarchy** | Plant → Line → Shift → Hour |
| **Shifts** | Morning 06:00–14:00 · Evening 14:00–22:00 · Night 22:00–06:00 (plant local time) |
| **Threshold** | Report only. Below 90% for a full shift is highlighted in the README charts. |
| **Owner** | Plant manager |
| **Query** | `sql/03_throughput_vs_plan.sql` |
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
| **Query** | `sql/04_scrap_cost_by_supplier.sql` |
| **Reading it** | The recoverable amount per supplier is the claim to file. The internal loss per station is the business case for fixing it. |

## Shared definitions

| Term | Definition |
|---|---|
| `dt` | Event date in UTC (`event_time`), the partition of every raw, curated and gold table |
| Consumed units | Sum of `quantity` for `CONSUMED` events |
| Rejected units | Sum of `quantity` for `REJECTED` events |
| Produced units | Sum of `quantity` for `PRODUCED` events (end of line) |
| Duplicate | Two records with the same `event_id`. Only the first one counts, everywhere. |
| Invalid record | Missing field, unknown part or station, or quantity ≤ 0. Goes to `quarantine/` with a reason. |
