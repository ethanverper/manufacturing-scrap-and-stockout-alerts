# Configuration and setup

Everything you can change, where it lives, and what to do after changing it. If you are running this project for the first time, read sections 1 and 2, then follow section 6.

## 1. Prerequisites

| Tool | Version | Why | Check |
|---|---|---|---|
| Python | ≥ 3.10 | Generator, scripts, tests | `python3 --version` |
| Java | 17 (Spark 3.5's supported version) | Local Spark for the Glue job tests and `make kpis-local`. Without it those tests are skipped. | `java -version` |
| Terraform | ≥ 1.6 | Creates the AWS resources | `terraform version` |
| AWS CLI | v2 | Uploads, streaming helpers, credentials | `aws --version` |
| AWS credentials | An IAM user or SSO profile allowed to create the services in section 5 | Terraform and the scripts act as you | `aws sts get-caller-identity` |

```bash
make install   # Python dependencies (boto3, pyyaml, pytest, ruff, moto, pyspark 3.5 ≈ 300 MB)
```

**Installing Terraform on a Mac with Apple Silicon (M1–M4):** use the native **arm64** build. If Homebrew lives in `/usr/local` (an Intel/Rosetta install), `brew install terraform` installs the Intel build, whose ~700 MB AWS provider fails with *"timeout while waiting for plugin to start"*. Install the official arm64 binary instead and verify its checksum:
```bash
V=1.17.1
curl -LO https://releases.hashicorp.com/terraform/$V/terraform_${V}_darwin_arm64.zip
curl -LO https://releases.hashicorp.com/terraform/$V/terraform_${V}_SHA256SUMS
grep darwin_arm64 terraform_${V}_SHA256SUMS | shasum -a 256 -c -
unzip terraform_${V}_darwin_arm64.zip && install -m 0755 terraform /usr/local/bin/terraform
terraform version   # must say: on darwin_arm64
```

`infra/terraform/.terraform.lock.hcl` pins the provider versions and checksums (macOS arm64 and Linux for CI). It is committed on purpose.

## 2. Configuration files at a glance

| File | In git? | What it holds | Who reads it |
|---|---|---|---|
| `config/settings.yaml` | ✅ yes | Every tunable value: alert thresholds, plant layout, simulation, AWS sizing | Python (`src/common/settings.py`) and Terraform (`yamldecode`) |
| `config/master_data.yaml` | ✅ yes | Suppliers, parts (cost, delivery cadence), bill of materials | Python (`src/generators/reference.py`) |
| `config/subscribers.yaml` | ❌ **no** (gitignored) | Who receives which alerts (real email addresses) | Terraform |
| `config/subscribers.example.yaml` | ✅ yes | Template for the file above | You (copy it) |

**Single source of truth:** a value lives in exactly one of these files. Terraform passes the values the AWS components need as Lambda environment variables (section 4), so the code in AWS and the code on your laptop always use the same numbers.

## 3. What each file contains

### `config/settings.yaml`

| Section | Keys | Change it when… |
|---|---|---|
| `project` | `name`, `region`, `plant_utc_offset_hours` | Deploying under another name or region (the name prefixes every AWS resource) |
| `alerts` | `reject_rate_baseline_pct`, `reject_rate_sigma`, `reject_rate_floor_pct`, `reject_rate_min_rejects`, `stock_coverage_threshold_days` | The business wants more or fewer alerts. See `docs/kpi-dictionary.md` and ADR-010 in `docs/decisions.md` |
| `plant` | plants, lines, station types, batch size, shifts, delivery hour, pallet size | Simulating a different plant layout |
| `simulation` | `seed`, `history_days`, defect rates, the 3 `incidents`, `noise`, `live_session` | Changing the story in the data, or its volume |
| `aws` | `kinesis`, `lambda`, `dynamodb`, `firehose` (buffer size/interval, `log_retention_days`), `glue` (`glue_version`, workers, `timeout_minutes`, `schedule_cron`, `schedule_enabled`), `athena` | Sizing, timeouts, retention (defaults are the cheapest that work). `schedule_enabled: true` turns the nightly job on (it bills every night). |

### `config/master_data.yaml`

| Key | Structure |
|---|---|
| `suppliers` | `SUPPLIER-ID: Display name` |
| `parts` | `PART-ID: {supplier, unit_cost, delivery_every_days, target_coverage_days}` |
| `station_bom` | `STATION-TYPE: {PART-ID: units per finished product}`. Every part must appear in exactly one station (a test enforces it) |

### `config/subscribers.yaml` (alert recipients)

Create it from the template:
```bash
cp config/subscribers.example.yaml config/subscribers.yaml
```

Structure:
```yaml
subscribers:
  - email: purchasing@example.com   # required: where alerts go
    role: Purchasing                # required: documentation, and part of the subscription key
    alerts: [LOW_STOCK]             # optional: LOW_STOCK and/or HIGH_REJECT_RATE. Omit = all kinds
    plants: [PLANT-MTY]             # optional: PLANT-MTY and/or PLANT-SLP. Omit = all plants
```

How it works:
1. Terraform creates **one SNS email subscription per entry**. Its **filter policy** is built from `alerts` (matched against the message attribute `kind`) and `plants` (matched against `plant`).
2. The Lambda publishes every alert with those two attributes (`src/lambdas/realtime_alerts/notifier.py`). SNS delivers each alert only to the subscriptions whose filter matches. The Lambda never knows who is subscribed.
3. After `make deploy`, **each address receives "AWS Notification – Subscription Confirmation". Nothing is delivered until its link is clicked.**
4. Every alert email has an *unsubscribe* link, so recipients can opt out themselves.

| To… | Do |
|---|---|
| Add a person | Add an entry, `make deploy`, and the person confirms the email |
| Change what someone receives | Edit their `alerts` / `plants`, then `make deploy` |
| Remove a person | Delete the entry, then `make deploy` |
| Check what is applied | `terraform -chdir=infra/terraform output alert_subscriptions` (email → filter policy) |

Without this file, the stack still deploys: alerts are published to the topic, but nobody receives them.

## 4. Values passed to AWS components

The Lambda reads only environment variables. Terraform sets them in `infra/terraform/2_realtime.tf`:

| Environment variable | Source |
|---|---|
| `EVENTS_SEEN_TABLE`, `PART_STOCK_TABLE`, `STATION_HOURLY_TABLE` | Table names created by Terraform |
| `EVENTS_SEEN_TTL_DAYS` | `aws.dynamodb.events_seen_ttl_days` |
| `ALERT_TOPIC_ARN` | The SNS topic created by Terraform |
| `REJECT_RATE_BASELINE_PCT` | `alerts.reject_rate_baseline_pct` |
| `REJECT_RATE_SIGMA` | `alerts.reject_rate_sigma` |
| `REJECT_RATE_FLOOR_PCT` | `alerts.reject_rate_floor_pct` |
| `REJECT_RATE_MIN_REJECTS` | `alerts.reject_rate_min_rejects` |
| `STOCK_COVERAGE_THRESHOLD_DAYS` | `alerts.stock_coverage_threshold_days` |

If any of them is missing, the Lambda refuses to start (`KeyError` at start-up) rather than alerting with a made-up default.

The Glue job reads only its job arguments. Terraform sets them in `infra/terraform/4_batch.tf` (`default_arguments`); locally, `scripts/kpis_local.py` builds the same list:

| Job argument | Source |
|---|---|
| `--input_root`, `--output_root` | `s3://<bucket>` on AWS; `data/generated` and `data/lake-local` locally |
| `--utc_offset_hours` | `project.plant_utc_offset_hours` |
| `--shift_start_hours` | `plant.shift_start_hours` (as JSON) |
| `--coverage_threshold_days` | `alerts.stock_coverage_threshold_days` |
| `--extra-py-files` | `s3://<bucket>/scripts/glue_libs.zip` (the job's modules, zipped by Terraform) |

Firehose takes `aws.firehose.*` (buffering, log retention) in `infra/terraform/3_landing.tf`. Its S3 prefix `raw/plant_events/dt=…/` is the lake layout, not a setting.

## 5. Private and generated files (never committed)

| Path | What | Recreate with |
|---|---|---|
| `config/subscribers.yaml` | Real email addresses | `cp config/subscribers.example.yaml config/subscribers.yaml` |
| `data/generated/` | Simulated history, master data CSVs (parts, plan, stations, opening stock), stock snapshot, live session | `make generate` |
| `data/lake-local/` | Output of the KPI job run locally (curated, gold, quarantine) | `make kpis-local` |
| `infra/terraform/.terraform/`, `*.tfstate*` | Terraform's provider cache and record of what it created | `terraform init` / created by `apply` |
| `infra/terraform/build/` | The Lambda zip and the Glue modules zip | Built automatically by `terraform apply` |

**Keep `*.tfstate` until you run `make destroy`.** It is how Terraform knows what to delete.

## 6. Running it, in order

| Step | Command | What happens | Cost |
|---|---|---|---|
| 1 | `make test` | Unit + simulated-AWS (moto) + local Spark tests (~4 min on a laptop) | $0 |
| 2 | `make generate` | 7 days of history + master data into `data/generated/` | $0 |
| 2b | `make kpis-local` (optional) | Runs the Glue job on that data with a local Spark, into `data/lake-local/` | $0 |
| 3 | `make deploy` | Creates the stream, Lambda, tables, topic and subscriptions, Firehose, the Glue job (schedule off) and the bucket. **Confirm the subscription emails.** | Stream ~$0.015/hour from here on |
| 4 | `make seed` | Loads the end-of-history stock into DynamoDB | ~$0 |
| 5 | `make upload-history` | History and master data to S3 | ~$0 |
| 6 | `make stream` | ~1,000 live events over ~10 minutes. Alert emails arrive within seconds; Firehose lands them in `raw/` within ~1 minute | ~$0 |
| 6b | `make kpis` | Starts the Glue job on everything in `raw/` (history + live). Check it in the Glue console; ~3 minutes | ~$0.05 per run |
| 7 | `make destroy` then `make leftovers` | Deletes everything, then checks that nothing billable is left | — |

## 7. Common changes

| I want to… | Edit | Then |
|---|---|---|
| Alert at a different reject threshold | `config/settings.yaml` → `alerts` | `make test` (some tests assert the documented values), then `make deploy` |
| Change who gets alerts | `config/subscribers.yaml` | `make deploy` |
| Change part costs or the BOM | `config/master_data.yaml` | `make generate`, `make upload-history` |
| Change the story (another bad lot, another shortage) | `config/settings.yaml` → `simulation.incidents` | `make generate`, `make test` |
| Use another AWS region | `config/settings.yaml` → `project.region` | `make destroy` in the old region first, then `make deploy` |
