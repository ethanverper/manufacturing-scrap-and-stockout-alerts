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
- **Consequences:** in "production" the trigger is turned on with one variable (`enable_schedule = true`).
