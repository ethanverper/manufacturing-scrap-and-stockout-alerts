# Deploy log

The real deploy of this project to AWS: what was created, what was checked at each step, what went wrong and how it was fixed, what it cost, and proof that everything was deleted afterwards.

- **Date:** 2026-10-30 (UTC evening) · **Region:** us-east-1
- **Billing window:** 23:38:56 → 00:20:42 UTC, **41 min 46 s** (cap: 2 h)
- **Estimated cost:** ≈ $0.15 (`docs/spec.md`, section 5) · **Actual cost: $0.12** (section 5)
- **Evidence:**
  - CLI output in `docs/evidence/<phase>/`, with the account id and emails replaced by placeholders;
  - console screenshots in `docs/images/console/`, cropped and redacted;
  - Athena results and charts in `docs/images/`.

## 1. Timeline

| Time (UTC) | Step | Command | Expected | Result |
|---|---|---|---|---|
| 23:30 | Pre-flight | `make test` · `make generate` · `terraform plan` | tests pass · 41 resources to add | ✅ 88 tests · 53,126 history events · 1,011 live events ready · `Plan: 41 to add` |
| 23:38:56 | **Create** | `make deploy` | `41 added` | ✅ `41 added` in 65 s |
| 23:40 | Evidence | `make evidence PHASE=deployed` | every resource as designed | ✅ 14 files (section 2) |
| 23:41 | Opening stock | `make seed` | 22 plant-part rows | ✅ 22 items in `part-stock` |
| 23:41 | History upload | `make upload-history` | raw/ and reference/ in S3 | ✅ 14 objects, 479 KB |
| 23:45 | Confirm subscriptions | email links | 2 confirmed | ✅ both confirmed |
| 23:50:02 | **Live session** | `make stream` | 1 low-stock + 7 reject-rate alerts, 0 errors | ✅ 1,011 events, 0 failed puts · **8 alerts** · 104 Lambda invocations, 0 errors, 0 throttles · DLQ empty |
| 23:58:42 | Landing | wait for Firehose | all live events in `raw/` | ✅ 9 GZIP files, **1,011 records** |
| 00:01:52 | Daily KPIs, run 1 | `make kpis` | SUCCEEDED | ❌ **FAILED** after 3 min 5 s: `AccessDenied` on `curated_$folder$` (section 4) |
| 00:08 | IAM fix | `terraform apply` | `1 changed` | ✅ `0 added, 1 changed, 0 destroyed` |
| 00:08:45 | Daily KPIs, run 2 | `make kpis` | SUCCEEDED, all days balanced | ✅ SUCCEEDED in 3 min 28 s · 54,137 = 53,541 + 537 + 59 |
| 00:15 | Queries and charts | `make charts` | 6 queries, 4 charts | ✅ 6 queries, **43 KB scanned in total** |
| 00:19:56 | **Destroy** | `make destroy` | `41 destroyed` | ✅ `41 destroyed` in 46 s |
| 00:21 | Glue logs | `make clean-glue-logs` | Glue's own log groups deleted | ✅ after a script fix (section 4): 3 log groups deleted |
| 00:21:31 | Cleanup check | `make leftovers` | `✅ Nothing billable left.` | ✅ clean on all three nets · `terraform state list`: 0 resources |

## 2. Checks after each phase

| Phase | Evidence | What it proves |
|---|---|---|
| Deployed | `docs/evidence/deployed/` | Each resource has the designed settings: <ul><li>Kinesis: 1 shard, KMS encryption, 24 h retention</li><li>Lambda: Python 3.12, 256 MB, 30 s, 10 environment variables; stream mapping with batch 100, bisect on error, 3 retries, partial failures</li><li>DynamoDB: 3 tables, on-demand</li><li>SNS: 2 email subscriptions with filter policies</li><li>Firehose: `raw/plant_events/dt=` prefix, GZIP</li><li>Glue: 5.0, 2 × G.1X, 10 min timeout, nightly trigger **created but not activated**</li><li>Athena: 8 tables, 1 GB enforced scan cap</li><li>S3: AES-256, all public access blocked</li></ul> |
| Streamed | `docs/evidence/streamed/` | The 8 alerts the Lambda sent, each with its numbers (below). 104 invocations, 0 errors, an empty dead-letter queue. CONNECTOR-C4 in Monterrey down to 0.894 days of stock. 9 Firehose files. |
| Batch | `docs/evidence/batch/` | The successful Glue run (208 s, 2 G.1X workers), its own reconciliation log line, and the gold Parquet files |
| Destroyed | `docs/evidence/destroyed/` | An empty Terraform state and a clean `leftovers` report |

**The 8 alerts, exactly the ones the local tests predicted:**

| Kind | Where | Hour (UTC) | Numbers |
|---|---|---|---|
| LOW_STOCK | PLANT-MTY · CONNECTOR-C4 (SUP-NORDCAB) | during the 06:00–09:00 session | 5,757 units = **0.999 days** (threshold 1.0) |
| HIGH_REJECT_RATE | PLANT-MTY · LINE-1-ASSEMBLY-3 | 12 · 13 · 14 | 6.25% · 6.25% · 8.16% vs control limits 5.28–5.87% |
| HIGH_REJECT_RATE | PLANT-MTY · LINE-2-ASSEMBLY-3 | 12 · 13 · 14 | 6.25% · 10.0% · 7.69% vs limits 5.25–5.82% |
| HIGH_REJECT_RATE | PLANT-MTY · LINE-3-ASSEMBLY-3 | 13 | 7.69% vs limit 5.25% |

All seven reject-rate alerts are on the ASSEMBLY-3 stations of Monterrey, the ones installing batteries from the defective lot LOT-7781. There were no false alarms (ADR-010). Each email went only to the person whose subscription filter matches it: Purchasing got the stock alert, Quality Monterrey got the rejects.

## 3. What the data showed (Athena, real run)

| Question | Answer | Evidence |
|---|---|---|
| Which lot sends defective parts? | **LOT-7781 (SUP-VOLTEX, BATTERY-A12): 13.87%** supplier defect rate. The next worst lot is at 1.68%. | `sql/02`, `docs/images/reject_rate_by_supplier_lot.png`, `docs/images/console/console-athena-rollup-results.png` |
| Which parts are running out? | **CONNECTOR-C4 in Monterrey: 0.89 days** at the end of the data. Every other plant-part has > 2.5 days. | `sql/03`, `docs/images/stock_coverage.png` |
| Which line and shift are behind plan? | **LINE-3 night shift: 89.1%** of plan. Every other line and shift is at 100% (after the fix in section 4). | `sql/04`, `docs/images/throughput_vs_plan.png` |
| What did scrap cost, and who pays? | **$239,125** in total, of which **$169,074 (71%) is recoverable** from suppliers. SUP-PRECISA costs the most ($131.9k), because its controllers are expensive, even though its defect rates are normal. SUP-VOLTEX's bad lot is the quality problem. | `sql/05`, `docs/images/scrap_cost_by_supplier.png` |
| Can we trust the numbers? | Every day reconciles: **54,137 raw = 53,541 curated + 537 duplicates + 59 quarantined** | `sql/06`, `docs/evidence/batch/glue-summary-log.txt` |

The same six SQL files were run on the same data with local Spark. They returned **identical results**; only the order of tied rows differed (ADR-013).

## 4. Differences from the plan

1. **Glue run 1 failed: IAM and the S3 folder markers.** The job processed all the data, then failed when writing, because Glue's S3 file system writes `curated_$folder$`-style marker objects next to each top-level output folder. The policy didn't allow them.
   - **Fix:** allow exactly those three objects, with no bucket-wide wildcard (`infra/terraform/4_batch.tf`). Run 2 succeeded.
   - **Why nothing broke:** the job writes nothing until the reconciliation passes, and `max_retries = 0` avoided an automatic paid retry.
   - Lesson 6 in `docs/lessons-learned.md`.
2. **Every night shift looked ~10% behind plan: a gap in the data read as zero production.** The history ends at 23:59 and the live session starts at 06:00. The 6 hours in between have no scans, and the throughput rule counted them as "planned, 0 produced": 90.3% on every night shift, 80.4% for LINE-3.
   - **Fix:** compare an hour with the plan only if the plant sent data in that hour (`build_throughput` in `src/glue_jobs/transforms.py`), with a regression test.
   - **Verification:** after the resources were destroyed, the fix was checked on the same data locally (history + the exact live session): 100% everywhere except LINE-3's night at 89.1%.
   - Both charts are kept: `docs/images/throughput_vs_plan_before_fix.png` is the real Athena output, `docs/images/throughput_vs_plan.png` is the corrected one.
   - Lesson 7.
3. **`make clean-glue-logs` deleted nothing on the first try.** The script stored the log group names in a variable called `GROUPS`, which bash reserves for the user's group ids. The loop tried to delete a log group named `20`. Renamed to `LOG_GROUPS` (`scripts/clean_glue_logs.sh`), re-run: 3 log groups deleted.

## 5. Actual cost

From Cost Explorer (`make cost-daily`), read on 2026-10-31. The account spends ~$0 otherwise, so this is the project's cost. Provisional until the last day settles; re-checked on 2026-11-02.

| Service | 2026-10-30 | 2026-10-31 | Total (USD) | Expected |
|---|---|---|---|---|
| AWS Glue (2 runs: the failed one and the successful one) | | 0.0966 | **0.0966** | ~0.10 |
| Amazon Kinesis Data Streams (1 shard, 42 min) | 0.0053 | 0.0050 | **0.0103** | ~0.015 |
| Amazon S3 | 0.0003 | 0.0051 | **0.0054** | < 0.01 |
| Amazon DynamoDB | 0.0038 | | **0.0038** | ~0.02 |
| Amazon Athena (6 queries, 43 KB scanned) | | 0.0004 | **0.0004** | < 0.01 |
| Amazon Data Firehose | 0.0001 | | **0.0001** | ~0.00 |
| Lambda, SNS, SQS, CloudWatch | | | 0.00 (free tier) | ~0.00 |
| **Total** | | | **$0.1167** | **≈ $0.15** |

Not included: $0.02 for two Cost Explorer API calls made while checking the cost ($0.01 each).

Takeaways:
- **Glue is 83% of the bill.** The failed run cost about as much as the successful one, which is why `max_retries = 0` matters: an automatic retry would have doubled the waste.
- **Kinesis, the only service billed by the hour, cost one cent**, because the deploy window was 42 minutes.

## 6. Teardown proof

- `terraform destroy`: `41 destroyed`
- `terraform state list`: **0 resources** (`docs/evidence/destroyed/terraform-state.txt`)
- `make leftovers`: **✅ Nothing billable left.** (`docs/evidence/destroyed/leftovers.txt`). It checks:
  - every resource still tagged `Project=manufacturing-scrap-and-stockout-alerts`;
  - 13 resource types by name;
  - 11 services that bill by the hour.
- Glue's own log groups: deleted with `make clean-glue-logs`.
- Cost Explorer: charges only on 2026-10-30 and 2026-10-31 (the deploy crossed midnight UTC). None expected after; re-checked on 2026-11-02.
