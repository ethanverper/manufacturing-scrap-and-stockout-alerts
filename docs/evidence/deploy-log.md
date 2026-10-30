# Deploy log

The real deploy of this project to AWS: what was created, what was checked at each step, what happened, what it cost, and proof that everything was deleted afterwards.

- **Date:** _YYYY-MM-DD_ · **Region:** us-east-1 · **Window:** _start_ → _end_ UTC (_N_ minutes, cap 2 h)
- **Estimated cost:** ≈ $0.15 (`docs/spec.md`, section 5) · **Actual cost:** _see section 5_
- **Evidence:** CLI output in `docs/evidence/<phase>/` (account id and emails redacted), console screenshots in `docs/images/`

## 1. Timeline

| Time (UTC) | Step | Command | Expected | Result |
|---|---|---|---|---|
| | Pre-flight | `make test` · `make generate` · `terraform plan` | tests pass · 41 resources to add | |
| | Create | `make deploy` | `41 added` | |
| | Confirm subscriptions | email links | 2 subscriptions confirmed | |
| | Opening stock | `make seed` | 22 plant-part rows in `part-stock` | |
| | History upload | `make upload-history` | raw/ and reference/ in S3 | |
| | Live session | `make stream` | 1 low-stock + 7 reject-rate alerts, 0 Lambda errors, empty DLQ | |
| | Landing | wait ~2 min | live files in `raw/plant_events/` | |
| | Daily KPIs | `make kpis` | run SUCCEEDED, all days balanced | |
| | Queries and charts | `make charts` | 6 queries, 4 charts in `docs/images/` | |
| | Destroy | `make destroy` | `41 destroyed` | |
| | Glue logs | `make clean-glue-logs` | Glue's own log groups deleted | |
| | Cleanup check | `make leftovers` | `✅ Nothing billable left.` | |

## 2. Checks after each phase

| Phase | Evidence folder | What it proves |
|---|---|---|
| Deployed | `docs/evidence/deployed/` | Every resource exists with the designed settings: 1 shard, Lambda limits, on-demand tables, filtered subscriptions, Firehose prefix and GZIP, Glue 5.0 with 2 workers and the trigger **disabled**, 8 Athena tables, 1 GB scan cap, bucket encrypted and private |
| Streamed | `docs/evidence/streamed/` | The alerts the Lambda sent, invocations vs errors, an empty dead-letter queue, live stock in DynamoDB, files Firehose landed |
| Batch | `docs/evidence/batch/` | The Glue run (state, duration, workers), its reconciliation line, the gold files |
| Destroyed | `docs/evidence/destroyed/` | Empty Terraform state and a clean `leftovers` report |

## 3. What the data showed

_Filled in from the Athena results (`docs/images/results/*.csv`) and the charts._

## 4. Differences from the plan

_Anything that behaved differently from the local tests, and what was changed (also recorded in `docs/lessons-learned.md`)._

## 5. Actual cost

_From `make cost-daily` the day after (Cost Explorer lags up to 24 h)._

| Service | Cost (USD) |
|---|---|
| | |
| **Total** | |

## 6. Teardown proof

- `terraform state list`: _0 resources_
- `make leftovers`: _✅ Nothing billable left._
- Tag Editor search `Project=manufacturing-scrap-and-stockout-alerts`: _0 resources_ (`docs/images/console-cleanup.png`)
- Cost Explorer two days later: _no charges after the deploy day_
