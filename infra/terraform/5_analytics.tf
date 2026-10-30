# =============================================================================
# 5_analytics.tf — Athena: the lake's tables in the Glue Data Catalog, and a capped workgroup.
#
# Athena doesn't store data. The catalog tells it, for each table, which S3 folder to read, the
# columns, and the file format (the SerDe). Every table is partitioned by dt and uses PARTITION
# PROJECTION: Athena computes the dt=... folders from a date range instead of asking a crawler or
# running MSCK REPAIR, so new days are queryable the moment Firehose or Glue writes them.
# =============================================================================

locals {
  # Athena database names can't contain hyphens
  database_name = replace(local.project, "-", "_")

  # The lake's tables: S3 folder (under the bucket), file format, and columns with Athena types.
  # Naming convention: table name = folder with "/" replaced by "_" (gold/reject_rate -> gold_reject_rate);
  # src/analytics/local_spark.py relies on it to give local tables the same names.
  # The types match what src/glue_jobs/transforms.py writes (Spark double -> double, long -> bigint).
  lake_tables = {
    raw_plant_events = {
      description = "Every scan exactly as the scanners sent it (Firehose + history). Duplicates and bad records included."
      folder      = "raw/plant_events"
      format      = "json"
      columns = {
        event_id    = "string", event_time = "string", event_type = "string", plant = "string", line = "string",
        station     = "string", part = "string", supplier = "string", lot = "string", quantity = "double",
        reason_code = "string"
      }
    }
    curated_events_clean = {
      description = "One row per physical scan: validated, deduplicated, enriched with cost and shift."
      folder      = "curated/events_clean"
      format      = "parquet"
      columns = {
        event_id      = "string", event_time = "string", event_ts = "timestamp", event_type = "string",
        plant         = "string", line = "string", station = "string", part = "string", supplier = "string",
        supplier_name = "string", lot = "string", quantity = "double", reason_code = "string",
        unit_cost     = "double", stock_delta = "double", local_date = "string", local_hour = "int", shift = "string"
      }
    }
    quarantine_plant_events = {
      description = "Records that broke a validation rule, with the first rule they broke."
      folder      = "quarantine/plant_events"
      format      = "parquet"
      columns = {
        event_id    = "string", event_time = "string", event_type = "string", plant = "string", line = "string",
        station     = "string", part = "string", supplier = "string", lot = "string", quantity = "double",
        reason_code = "string", quarantine_reason = "string"
      }
    }
    gold_reject_rate = {
      description = "KPI 1. Grain: dt, plant, line, station, part, supplier, lot. Rate = rejected / (consumed + rejected)."
      folder      = "gold/reject_rate"
      format      = "parquet"
      columns = {
        plant                = "string", line = "string", station = "string", part = "string", supplier = "string", lot = "string",
        consumed_units       = "double", rejected_units = "double", supplier_defect_units = "double",
        process_defect_units = "double"
      }
    }
    gold_stock_coverage = {
      description = "KPI 2. Grain: dt, plant, part, supplier. End-of-day stock and coverage in days."
      folder      = "gold/stock_coverage"
      format      = "parquet"
      columns = {
        plant              = "string", part = "string", supplier = "string", planned_daily_usage = "double",
        received_units     = "double", consumed_units = "double", rejected_units = "double",
        net_change_units   = "double", closing_units = "double", coverage_days = "double",
        is_below_threshold = "boolean"
      }
    }
    gold_throughput = {
      description = "KPI 3. Grain: dt, plant, line, local date and hour (+ shift). Throughput = produced / planned."
      folder      = "gold/throughput"
      format      = "parquet"
      columns = {
        plant         = "string", line = "string", local_date = "string", local_hour = "int", shift = "string",
        planned_units = "double", produced_units = "double"
      }
    }
    gold_scrap_cost = {
      description = "KPI 4. Grain: dt, plant, line, station, part, supplier, reason. Recoverable = supplier defect."
      folder      = "gold/scrap_cost"
      format      = "parquet"
      columns = {
        plant       = "string", line = "string", station = "string", part = "string", supplier = "string",
        reason_code = "string", rejected_units = "double", scrap_cost_usd = "double", is_recoverable = "boolean"
      }
    }
    gold_data_quality = {
      description = "Record reconciliation per day: raw = curated + duplicates + quarantined."
      folder      = "gold/data_quality"
      format      = "parquet"
      columns = {
        raw_records       = "bigint", curated_records = "bigint", quarantined_records = "bigint",
        duplicate_records = "bigint", is_balanced = "boolean"
      }
    }
  }

  # How Athena reads each file format: input/output classes and the SerDe (serializer/deserializer)
  formats = {
    parquet = {
      input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    json = {
      # Plain text files (gzip is detected from the .gz extension), one JSON object per line
      input_format  = "org.apache.hadoop.mapred.TextInputFormat"
      output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
      serde         = "org.openx.data.jsonserde.JsonSerDe"
    }
  }
}

# ---------------------------------------------------------------------------
# 1. The database (a folder of table definitions in the Glue Data Catalog; free)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "lake" {
  name        = local.database_name
  description = "Plant scrap and stock-out data lake: raw, curated, quarantine and one gold table per KPI."
}

# ---------------------------------------------------------------------------
# 2. One table per lake folder, all with partition projection on dt
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_table" "lake" {
  for_each = local.lake_tables

  name          = each.key
  database_name = aws_glue_catalog_database.lake.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE" # Athena only reads the files; dropping the table never deletes data

  # dt is a folder (dt=2026-10-28), not a column inside the files
  partition_keys {
    name = "dt"
    type = "string"
  }

  parameters = {
    # Partition projection: dt is any date from N days ago to tomorrow, in the folder format below
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "NOW-${local.aws.athena.projection_days_back}DAYS,NOW+1DAYS"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "storage.location.template"   = "s3://${aws_s3_bucket.lake.bucket}/${each.value.folder}/dt=$${dt}/"
    "classification"              = each.value.format
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/${each.value.folder}/"
    input_format  = local.formats[each.value.format].input_format
    output_format = local.formats[each.value.format].output_format

    ser_de_info {
      serialization_library = local.formats[each.value.format].serde
    }

    # The columns, in the order declared above
    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

# ---------------------------------------------------------------------------
# 3. The workgroup: where results go, and a hard limit on what one query may scan (and cost)
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "analytics" {
  name          = local.project
  force_destroy = true # destroy also removes the saved query history

  configuration {
    # Clients can't override these settings: the scan limit always applies (docs/spec.md, cost)
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Athena cancels any query that would scan more than 1 GB (config: aws.athena)
    bytes_scanned_cutoff_per_query = local.aws.athena.bytes_scanned_cutoff_per_query

    # Results land in athena-results/, which a lifecycle rule (0_main.tf) deletes after 7 days
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
