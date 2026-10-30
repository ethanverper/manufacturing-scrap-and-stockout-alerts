# Design decisions

## ADR-001: Kinesis Data Streams in front, not Firehose alone
- **Context:** the same scans feed real-time alerts (seconds) and the data lake.
- **Decision:** Kinesis Data Streams, with two consumers: Lambda and Firehose.
- **Alternatives:** Firehose only. It is simpler and cheaper, but it has a single destination, no replay, and always waits for its buffer, so alerts would be late.
- **Consequences:** one stream billed by the hour (~$0.015). It must be destroyed after the demo.

## ADR-002: Provisioned mode with 1 shard
- **Context:** the demo sends ~10 events per second.
- **Decision:** provisioned, 1 shard (1 MB/s or 1,000 records/s of input).
- **Alternatives:** on-demand. It scales automatically, but costs more per hour and adds nothing at this volume.
- **Consequences:** a real plant with thousands of scanners would need more shards or on-demand mode.

## ADR-003: Idempotency with a DynamoDB transaction
- **Context:** Kinesis delivers at least once, and scanners re-send after timeouts. In learning level 8, a duplicate produced two identical alert emails.
- **Decision:** `TransactWriteItems` that puts `event_id` into `events_seen` (with `attribute_not_exists`) **and** updates stock and the hourly counters, all in one transaction.
- **Alternatives:**
  - Check first, then write: two steps, so a crash between them loses or double counts.
  - Deduplicate only in batch: alerts would still be duplicated.
- **Consequences:** transactional writes cost 2× a normal write (still cents). `events_seen` uses TTL so it doesn't grow forever.

## ADR-004: "Last hour" reject rate with DynamoDB counters, not Flink
- **Context:** the alert needs a time window per station.
- **Decision:** clock-hour buckets in `station_hourly`, using atomic `ADD`. The alert evaluates the current hour, with a minimum of 20 units.
- **Alternatives:** Managed Service for Apache Flink, with true sliding windows. It is more precise, but costs at least ~$0.22 per hour and is much more complex. It is planned for a later project (card fraud).
- **Consequences:** right after the hour changes the bucket is small, which the 20-unit minimum handles. Windows are aligned to the clock, not sliding.

## ADR-005: Stock tracked as deltas with `ADD`
- **Context:** events can arrive late or out of order.
- **Decision:** each event adds a signed delta to `on_hand` (+received, −consumed, −rejected). The opening stock is seeded from reference data.
- **Alternatives:** scanners send the absolute stock. A late event would then overwrite a newer level.
- **Consequences:** order doesn't matter for the totals. The stock is only as good as the opening seed, and the batch job recomputes the end-of-day position independently, so the two can be compared.

## ADR-006: Raw layer kept untouched, quality enforced in batch
- **Context:** rules and KPIs will change, and bugs happen.
- **Decision:** Firehose lands every record as received (GZIP JSON). Validation, deduplication and enrichment happen in the Glue job, and invalid rows go to `quarantine/`.
- **Alternatives:** transforming to Parquet inside Firehose. It is cheaper to query, but the original record is lost and the bad ones are harder to quarantine.
- **Consequences:** any day can be reprocessed from raw. The Glue job overwrites only the dates it processes, so re-runs are safe.

## ADR-007: KPIs stored at the lowest grain, rolled up in SQL
- **Context:** each KPI has a 3–4 level hierarchy.
- **Decision:** one gold table per KPI at its lowest grain. `GROUP BY ROLLUP` in Athena produces every hierarchy level in one query.
- **Alternatives:** precomputing every level. That means more tables, more code, and a risk of the levels not adding up.
- **Consequences:** totals always reconcile across levels, because they come from the same rows.

## ADR-008: No QuickSight; charts generated from Athena results
- **Context:** QuickSight is a monthly subscription, which breaks the < $1 project budget.
- **Decision:** `scripts/make_charts.py` runs the `sql/` queries and saves PNG charts into `docs/images/` for the README.
- **Consequences:** the charts are static, but readable on GitHub by anyone.

## ADR-009: Nightly trigger disabled by default
- **Context:** a forgotten schedule keeps billing every night.
- **Decision:** the Glue trigger is created with `enabled = false`, and the demo runs the job on demand (`make kpis`).
- **Consequences:** in "production" the trigger is turned on with one setting (`aws.glue.schedule_enabled: true` in `config/settings.yaml`).

## ADR-010: Reject-rate alert uses a p-chart control limit, not a fixed 3%
- **Context:** the first rule (fixed 3%, minimum 20 units) was run against the live session locally. It raised **22 alerts, only 8 of them from the real problem** (the bad Voltex lot). Over the 7-day history it raised 863. Stations handle very different volumes (ASSEMBLY-3 ~60 parts/hour, ASSEMBLY-2 ~200), so with few parts a single reject already reads as "3.2%". Too many false alarms would teach Purchasing and Quality to ignore the emails, including the real ones.
- **Decision:** statistical process control, as used on factory floors. The alert fires when the station's hourly rate is above its **upper control limit** `p̄ + 3·√(p̄(1−p̄)/n)`, **and** above a 3% floor, **and** with at least 4 rejected units.
- **Result (local test on the same data):** **live session: 7 alerts, all 7 from the bad lot** (before: 22, with 14 false). 7-day history: 66 alerts (before: 863).
- **Alternatives:** raising the fixed percentage (it misses the bad lot on high-volume stations), or a minimum of rejects only (it still raised 259 alerts over the history).
- **Consequences:** the baseline p̄ (1.2%) is a setting. In production the nightly Glue job would recompute it per station from the last 30 days, a natural link between the batch and real-time paths.

## ADR-011: The KPI job rebuilds every day on each run (full refresh), by event date
- **Context:** the end-of-day stock position is a running sum: opening stock plus every movement since. A job that processed only "yesterday" would need yesterday's closing position as state.
- **Decision:** each run reads all raw events (~53,000 in the demo) and rewrites every table with **dynamic partition overwrite**, so only the dates present are replaced and a re-run gives identical tables. Dates come from the event's own `event_time` (UTC), **not** from the `dt=` folder, because Firehose names folders by arrival time and a scan at 23:59:59 can arrive after midnight. Glue job bookmarks are disabled.
- **Alternatives:** incremental runs with job bookmarks, carrying the previous day's closing stock forward. That scales better, but adds state that can drift.
- **Consequences:** simple and idempotent at this size (the job runs in about 3 minutes on 2 workers). At production volume the next step is incremental processing, seeded from the previous day's `gold/stock_coverage`.

## ADR-012: Plain Spark on Glue 5.0, tested locally with the same PySpark
- **Context:** Glue offers DynamicFrames and a `GlueContext`, but they only exist inside Glue, which makes the code hard to test without paying for runs.
- **Decision:** the job uses plain Spark DataFrames on **Glue 5.0 (Spark 3.5)**. The transformations live in `src/glue_jobs/transforms.py` as pure functions; `src/glue_jobs/daily_kpis.py` only reads, calls them and writes. Tests and `make kpis-local` run the same code with **PySpark 3.5** on a laptop or in CI.
- **Alternatives:** DynamicFrames (useful for messy, changing schemas and job bookmarks, neither needed here), or developing directly against Glue (slow and billed per run).
- **Consequences:** the job is portable to EMR or Databricks unchanged, and every rule is tested before deploy. Local runs need Java 17 for Spark.

## ADR-013: Portable KPI SQL, run on Athena and tested on Spark
- **Context:** the KPI queries run on Athena (Trino). Testing them only after a deploy means paying for every mistake.
- **Decision:** the `sql/` files stick to SQL that both Athena and Spark SQL accept. Two things are avoided:
  - engine-specific `GROUPING()` functions. The hierarchy level is derived with `CASE WHEN <column> IS NULL`, which is safe because no dimension is null in the gold tables;
  - decimal-only arithmetic.

  `src/analytics/local_spark.py` registers the job's tables under their Athena names (folder with `/` → `_`), keeps `dt` as a string like the catalog does, and runs the same files. `tests/glue_jobs/test_sql.py` checks:
  - that every query runs;
  - that ROLLUP subtotals add up level by level;
  - that the planted story reads correctly in the results.
- **Alternatives:** separate test queries (they drift from the real ones), or testing only on Athena (it costs money and needs a deploy).
- **Consequences:** a mistake in a query is caught for free. Stock coverage is deliberately not rolled up: days of coverage are not additive.
