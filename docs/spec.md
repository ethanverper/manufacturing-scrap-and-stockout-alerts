# Project spec — Manufacturing scrap and stock-out alerts

> Written before any code. Every design choice in this repo traces back to this document.

## 1. Business case

- **Industry / function:** discrete manufacturing (assembly plants), plant operations, quality, purchasing and plant finance.
- **Question to answer:** *Where is the factory losing money to scrapped parts, which supplier or station is causing it, and are we about to stop a line because a part runs out?*
- **Who decides what with it:**

  | Role | Decision |
  |---|---|
  | Purchasing / material planner | Expedite an order before a part runs out |
  | Quality engineer | Stop and inspect a station, or quarantine a supplier lot |
  | Plant manager | Rebalance a line that is behind plan |
  | Plant controller (finance) | Size the scrap cost and charge supplier defects back to the supplier |

- **Cost of not knowing:**
  - A stopped assembly line costs thousands of dollars per hour. Today a stock-out is discovered when the bin is empty.
  - Scrap is reported once a month, so a bad supplier lot is consumed for weeks before anyone connects the rejects to it.

## 2. KPIs and hierarchy

| # | KPI | Formula | Hierarchy (L1 → L2 → L3 → L4) | Threshold / alert | Frequency |
|---|---|---|---|---|---|
| 1 | **Reject rate %** | rejected units ÷ (consumed + rejected units) | Plant → Line → Station → Part · Supplier → Part → Lot | **> 3% in the last hour** for a station → email Quality | Real time (hourly window) + daily |
| 2 | **Stock coverage (days)** | units on hand ÷ planned daily usage | Plant → Part → Supplier | **< 1 day** → email Purchasing | Real time |
| 3 | **Throughput vs plan %** | units produced ÷ planned units | Plant → Line → Shift → Hour | Report only (no alert) | Daily |
| 4 | **Scrap cost ($)** | rejected units × unit cost | Plant → Supplier → Part · Plant → Line → Station | Report only; supplier-caused scrap = chargeback candidate | Daily |

```
Reject rate %                    Stock coverage (days)       Throughput vs plan %      Scrap cost ($)
├─ Plant                         └─ Plant                    └─ Plant                  ├─ Plant
│  └─ Line                          └─ Part                     └─ Line                │  └─ Supplier
│     └─ Station                       └─ Supplier                 └─ Shift            │     └─ Part
│        └─ Part                                                       └─ Hour         │        (supplier vs process defect)
└─ Supplier                                                                           └─ Plant → Line → Station
   └─ Part
      └─ Lot
```

**Why supplier matters:** a reject has a **reason code**: `SUPPLIER_DEFECT` (the part arrived bad) or `PROCESS_DEFECT` (the station damaged it). That single field separates "call the supplier and charge them back" from "fix our station". Rejects also carry the supplier **lot**, so one bad lot can be quarantined without blocking the whole supplier.

## 3. Data

### How it arrives
| Source | Pattern | Volume in the demo |
|---|---|---|
| Station and warehouse scanners | **Event stream** → Kinesis Data Streams (one event per scan) | ~1,000 live events (06:00–09:00 plant time) streamed over ~10 minutes |
| Event history (7 days) | Backfill written straight to S3 raw (as if Firehose had delivered it) | ~53,000 events (2 plants × 3 lines × 24 h × 7 days) |
| Reference data: parts, suppliers, production plan | Small CSV files in S3 (master data from the ERP) | 11 parts, 4 suppliers, hourly plan per line |

### Event types (one schema, `event_type` decides which fields matter)
| `event_type` | Meaning | Key fields |
|---|---|---|
| `CONSUMED` | A station used parts | station, part, quantity |
| `REJECTED` | Parts failed inspection | station, part, supplier, lot, quantity, `reason_code` |
| `RECEIVED` | The warehouse received a delivery | part, supplier, lot, quantity |
| `PRODUCED` | Finished units left the end of a line | line, quantity |

### Sample record
```json
{
  "event_id": "EV-2026-10-28-000123",
  "event_time": "2026-10-28T10:15:42Z",
  "event_type": "REJECTED",
  "plant": "PLANT-MTY",
  "line": "LINE-2",
  "station": "ASSEMBLY-3",
  "part": "BATTERY-A12",
  "supplier": "SUP-VOLTEX",
  "lot": "LOT-7781",
  "quantity": 2,
  "reason_code": "SUPPLIER_DEFECT"
}
```

### Data issues the pipeline must handle
- **Duplicates:** a scanner re-sends after a network timeout (same `event_id`). This must not double count in real time or in batch.
- **Out-of-order events:** a late `RECEIVED` must not undo a newer stock level.
- **Bad records:** unknown part, negative quantity, missing fields. Reject them to an error location, never silently drop them.

### The story built into the data (so the demo shows real findings)
1. **Supplier VOLTEX lot LOT-7781** has a defect: reject rate on BATTERY-A12 at Monterrey jumps from ~1.2% to ~10% once the lot reaches the stations (FIFO: older stock is used first), pushing the ASSEMBLY-3 stations above 3% per hour in the live session. Most rejects are coded `SUPPLIER_DEFECT`.
2. **CONNECTOR-C4** is consumed faster than planned and its next delivery is late, so the live session starts at ~1.05 days of coverage and drops below 1 day around 06:50 plant time.
3. **LINE-3** runs ~11% behind plan on the night shift (89% of plan) (a process issue, not a supplier one).

## 4. Design

- **Pattern:** a mix of speeds.
  - **Real time (seconds):** alerts.
  - **Near real time (~1 min):** landing in S3.
  - **Batch (daily):** KPIs.
- **Services and why** (the detailed justification goes in the README):

  | Need | Service |
  |---|---|
  | Receive scanner events in order per part, several consumers, replay | **Kinesis Data Streams** (provisioned, 1 shard) |
  | Real-time rules: stock coverage and the 1-hour reject window | **Lambda** consumer |
  | Remember stock per part, hourly counters per station, processed `event_id`s | **DynamoDB** (on-demand) |
  | Email Purchasing and Quality | **SNS** |
  | Land every raw event in the lake, no code | **Data Firehose** → S3 `raw/` |
  | Daily KPIs over history: dedupe, join reference data, aggregate at every hierarchy level | **Glue (PySpark)** → S3 `curated/` (Parquet, partitioned by date) |
  | Ad-hoc questions and the evidence for the README | **Athena** (one query per KPI in `sql/`) |
  | Charts for the README | Generated locally from Athena results (no QuickSight) |
  | Infrastructure | **Terraform**, one file per stage |

- **New concepts in this project:**
  1. **Idempotency:** a DynamoDB conditional write on `event_id`, so a re-sent scan never counts twice, not even in real time. This fixes the duplicate emails seen in learning level 8.
  2. **A time-window metric without Flink:** hourly counters per station in DynamoDB (atomic `ADD`) to compute "reject rate in the last hour".
- **Depth:** Standard.

## 5. Cost estimate (goal < $1, hard cap $3)

| Service | Unit price (us-east-1) | Usage during the demo | Estimate |
|---|---|---|---|
| Kinesis Data Streams | $0.015 per shard-hour + $0.014 per million PUT units | 1 shard × 2 h; ~1,000 records | $0.03 |
| Lambda | $0.20 per million requests + compute | a few hundred invocations | ~$0.00 (free tier) |
| DynamoDB on-demand | ~$1.25 per million writes | ~10,000 writes | $0.02 |
| SNS email | first 1,000 emails free | ~10 emails | $0.00 |
| Data Firehose | $0.029 per GB (records rounded up to 5 KB) | ~3,000 × 5 KB ≈ 15 MB | ~$0.00 |
| S3 | $0.023 per GB-month | ~50 MB for a few hours | ~$0.00 |
| Glue (Spark) | $0.44 per DPU-hour, 1-minute minimum | 2 DPU × ~3 min × 2 runs | $0.09 |
| Athena | $5 per TB, 10 MB minimum per query | ~20 queries | ~$0.00 |
| CloudWatch Logs | $0.50 per GB ingested | a few MB, 1-day retention | ~$0.00 |
| **Total** | | | **≈ $0.15** |

- Deploy window ≤ 2 hours.
- The nightly Glue trigger is **disabled by default**; the demo runs the job on demand.
- After `make destroy`, `make leftovers` must print nothing.

## 6. Definition of done
- [ ] All code in English, modular, and commented per `docs/CODING_STANDARDS.md`
- [ ] Unit tests and CI green (ruff, pytest, terraform fmt/validate)
- [ ] Deployed for real: alert emails, Athena results and charts captured in `docs/images/`
- [ ] README section 3 shows the 3 findings from the data story, with numbers
- [ ] `make destroy` + `make leftovers` empty
- [ ] Actual cost recorded in the README (expected ≈ $0.15, ≤ $3)
- [ ] CV bullet drafted (portfolio project, described truthfully)
