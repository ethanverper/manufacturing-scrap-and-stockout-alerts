# =============================================================================
# 4_batch.tf — the daily Glue job (PySpark) that turns raw events into KPI tables.
#
#   raw/ + reference/ ──► src/glue_jobs/daily_kpis.py ──► curated/ · gold/<kpi>/ · quarantine/
#
# The job code is uploaded to scripts/ in the bucket. The schedule exists but is disabled by default
# (ADR-009); the demo runs the job on demand with `make kpis`.
# =============================================================================

# ---------------------------------------------------------------------------
# 1. The code: the entry script, and a zip with the modules it imports
# ---------------------------------------------------------------------------

# Glue runs this file as the job's main script
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/daily_kpis.py"
  source = "${local.repo_root}/src/glue_jobs/daily_kpis.py"
  etag   = filemd5("${local.repo_root}/src/glue_jobs/daily_kpis.py") # re-upload when the code changes
}

# The modules the script imports (transforms, the shared schema, JSON logging), with their package
# __init__ files so `from src.glue_jobs import transforms` works on Glue exactly as it does locally
data "archive_file" "glue_libs" {
  type        = "zip"
  output_path = "${path.module}/build/glue_libs.zip"

  dynamic "source" {
    for_each = toset([
      "src/__init__.py",
      "src/common/__init__.py",
      "src/common/schema.py",
      "src/common/logging_utils.py",
      "src/glue_jobs/__init__.py",
      "src/glue_jobs/transforms.py",
    ])

    content {
      content  = file("${local.repo_root}/${source.value}")
      filename = source.value
    }
  }
}

# Upload the zip; Glue adds it to the Python path through --extra-py-files
resource "aws_s3_object" "glue_libs" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/glue_libs.zip"
  source = data.archive_file.glue_libs.output_path
  etag   = data.archive_file.glue_libs.output_md5
}

# ---------------------------------------------------------------------------
# 2. IAM: read raw/ reference/ scripts/, write curated/ gold/ quarantine/
# ---------------------------------------------------------------------------

# The role the Glue job runs as
resource "aws_iam_role" "glue" {
  name = "${local.project}-glue"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS's base policy for Glue jobs: write logs and metrics to CloudWatch
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least privilege on the lake: read the inputs, rewrite only the outputs
resource "aws_iam_role_policy" "glue_lake" {
  name = "lake"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # List folders (Spark discovers files and partitions by listing)
        Sid      = "ListLake"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.lake.arn
      },
      {
        # Read raw events, master data and the job's own code
        Sid    = "ReadInputs"
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/raw/*",
          "${aws_s3_bucket.lake.arn}/reference/*",
          "${aws_s3_bucket.lake.arn}/scripts/*",
        ]
      },
      {
        # Write the outputs. Delete is needed to overwrite a date partition on a re-run.
        # The "<layer>_$folder$" objects are empty folder markers that Glue's S3 file system writes
        # next to each top-level output folder; without them the job fails with AccessDenied
        # (found on the first real run, docs/lessons-learned.md lesson 6).
        Sid    = "WriteOutputs"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/curated/*",
          "${aws_s3_bucket.lake.arn}/gold/*",
          "${aws_s3_bucket.lake.arn}/quarantine/*",
          "${aws_s3_bucket.lake.arn}/curated_$folder$",
          "${aws_s3_bucket.lake.arn}/gold_$folder$",
          "${aws_s3_bucket.lake.arn}/quarantine_$folder$",
        ]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# 3. The job
# ---------------------------------------------------------------------------

resource "aws_glue_job" "daily_kpis" {
  name     = "${local.project}-daily-kpis"
  role_arn = aws_iam_role.glue.arn

  # Spark job on the smallest setup: 2 G.1X workers (config: aws.glue), stopped after 10 minutes
  glue_version      = local.aws.glue.glue_version
  worker_type       = local.aws.glue.worker_type
  number_of_workers = local.aws.glue.number_of_workers
  timeout           = local.aws.glue.timeout_minutes
  max_retries       = 0 # a failed run is investigated, not silently repeated

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  # One run at a time: two runs overwriting the same partitions would race
  execution_property {
    max_concurrent_runs = 1
  }

  # The job's arguments: where the lake is, and the business settings from config/settings.yaml
  default_arguments = {
    "--extra-py-files"          = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_libs.key}"
    "--input_root"              = "s3://${aws_s3_bucket.lake.bucket}"
    "--output_root"             = "s3://${aws_s3_bucket.lake.bucket}"
    "--utc_offset_hours"        = tostring(local.settings.project.plant_utc_offset_hours)
    "--shift_start_hours"       = jsonencode(local.settings.plant.shift_start_hours)
    "--coverage_threshold_days" = tostring(local.alerts.stock_coverage_threshold_days)
    "--job-bookmark-option"     = "job-bookmark-disable" # full refresh every run (ADR-011)
  }

  # The policies must exist before the first run can start
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_lake]
}

# ---------------------------------------------------------------------------
# 4. The nightly schedule (created DISABLED: a forgotten schedule keeps billing, ADR-009)
# ---------------------------------------------------------------------------

resource "aws_glue_trigger" "nightly" {
  name     = "${local.project}-nightly"
  type     = "SCHEDULED"
  schedule = local.aws.glue.schedule_cron    # 07:00 UTC = 01:00 plant time, after the day closes
  enabled  = local.aws.glue.schedule_enabled # false unless changed in config/settings.yaml

  actions {
    job_name = aws_glue_job.daily_kpis.name
  }
}
