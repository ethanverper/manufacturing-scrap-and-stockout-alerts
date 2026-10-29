# =============================================================================
# 2_realtime.tf — the real-time path: Kinesis -> Lambda -> DynamoDB -> SNS email.
#
# Contents, in the order a record meets them:
#   1. DynamoDB tables      the Lambda's memory: processed events, stock, hourly counters
#   2. SNS topic            the alerts "mailing list", plus one filtered subscription per person
#   3. SQS dead-letter      batches that still fail after every retry wait here, nothing is lost
#   4. IAM role             exactly what the Lambda may do, and nothing else
#   5. Lambda               the code in src/lambdas/realtime_alerts/, packaged with src/common/
#   6. Event source mapping how Lambda reads the stream (batching, retries, partial failures)
# =============================================================================

# ---------------------------------------------------------------------------
# 1. DynamoDB tables (on-demand: pay per request, ~$0 for the demo)
# ---------------------------------------------------------------------------

# Idempotency guard: one row per processed event_id, and one per alert already sent ("ALERT#...").
# TTL deletes old rows for free, so the table never grows without limit.
resource "aws_dynamodb_table" "events_seen" {
  name         = "${local.project}-events-seen"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dedupe_key"

  attribute {
    name = "dedupe_key"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Live stock per plant and part ("PLANT-MTY#CONNECTOR-C4"), seeded by `make seed`
resource "aws_dynamodb_table" "part_stock" {
  name         = "${local.project}-part-stock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "plant_part"

  attribute {
    name = "plant_part"
    type = "S"
  }
}

# Consumed / rejected counters per station and UTC hour: the window of the reject-rate alert
resource "aws_dynamodb_table" "station_hourly" {
  name         = "${local.project}-station-hourly"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "plant_station"
  range_key    = "hour"

  attribute {
    name = "plant_station"
    type = "S"
  }

  attribute {
    name = "hour"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# ---------------------------------------------------------------------------
# 2. SNS: the alerts topic and one filtered email subscription per person
# ---------------------------------------------------------------------------

# The topic the Lambda publishes to. It doesn't know who is subscribed.
resource "aws_sns_topic" "alerts" {
  name = "${local.project}-alerts"
}

locals {
  # One entry per person in config/subscribers.yaml, keyed "<position>-<role>" (stable, readable)
  subscriptions = { for index, subscriber in local.subscribers : "${index}-${subscriber.role}" => subscriber }

  # Each person's SNS filter policy, from their optional `alerts` and `plants` lists. An empty list
  # adds no condition; an empty policy means "no filter" (the person receives every alert).
  subscription_filters = {
    for key, subscriber in local.subscriptions : key => merge(
      length(subscriber.alerts) > 0 ? { kind = subscriber.alerts } : {},
      length(subscriber.plants) > 0 ? { plant = subscriber.plants } : {},
    )
  }
}

# One email subscription per person, with their filter policy. The Lambda tags every alert with
# "kind" and "plant" (src/lambdas/realtime_alerts/notifier.py); SNS delivers it only where the
# filter matches. Each address gets a confirmation email first: nothing arrives until it's clicked.
resource "aws_sns_topic_subscription" "email" {
  for_each = local.subscriptions

  topic_arn     = aws_sns_topic.alerts.arn
  protocol      = "email"
  endpoint      = each.value.email
  filter_policy = length(local.subscription_filters[each.key]) == 0 ? null : jsonencode(local.subscription_filters[each.key])
}

# ---------------------------------------------------------------------------
# 3. Dead-letter queue: where batches go after every retry failed
# ---------------------------------------------------------------------------

# Records land here only if they keep failing for a transient reason (bad data is skipped, not
# retried). Holding them for 4 days leaves time to fix the cause and replay them.
resource "aws_sqs_queue" "realtime_dlq" {
  name                      = "${local.project}-realtime-dlq"
  message_retention_seconds = 345600 # 4 days
  sqs_managed_sse_enabled   = true   # encrypted at rest at no extra cost
}

# ---------------------------------------------------------------------------
# 4. IAM: the Lambda's "badge"
# ---------------------------------------------------------------------------

# Who may wear the badge: only the Lambda service
resource "aws_iam_role" "realtime_lambda" {
  name = "${local.project}-realtime-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS-managed policy: read the Kinesis stream and write CloudWatch logs
resource "aws_iam_role_policy_attachment" "realtime_kinesis" {
  role       = aws_iam_role.realtime_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
}

# Least privilege for everything else: only these three tables, this topic, this queue
resource "aws_iam_role_policy" "realtime_access" {
  role = aws_iam_role.realtime_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # The transaction (Put + Update + ConditionCheck), reads, and the alert claims
        Sid      = "State"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem"]
        Resource = [aws_dynamodb_table.events_seen.arn, aws_dynamodb_table.part_stock.arn, aws_dynamodb_table.station_hourly.arn]
      },
      {
        Sid      = "Alerts"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      },
      {
        # The event source mapping sends failed batches here on the Lambda's behalf
        Sid      = "DeadLetter"
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.realtime_dlq.arn
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# 5. The Lambda
# ---------------------------------------------------------------------------

# Package only what the Lambda runs: src/common/ and src/lambdas/realtime_alerts/ (plus the package
# __init__ files). The generators and scripts stay out. boto3 is already in the Lambda runtime.
data "archive_file" "realtime_lambda" {
  type        = "zip"
  output_path = "${path.module}/build/realtime_alerts.zip"

  dynamic "source" {
    for_each = toset(concat(
      ["src/__init__.py", "src/lambdas/__init__.py"],
      [for file in fileset(local.repo_root, "src/common/*.py") : file if file != "src/common/settings.py"],
      tolist(fileset(local.repo_root, "src/lambdas/realtime_alerts/*.py")),
    ))

    content {
      content  = file("${local.repo_root}/${source.value}")
      filename = source.value
    }
  }
}

# Logs kept 1 day (config: aws.lambda.log_retention_days); created here so destroy removes them too
resource "aws_cloudwatch_log_group" "realtime_lambda" {
  name              = "/aws/lambda/${local.project}-realtime-alerts"
  retention_in_days = local.aws.lambda.log_retention_days
}

# The function. Every threshold and table name arrives as an environment variable, taken from
# config/settings.yaml: the YAML is the single source of truth (docs/configuration.md).
resource "aws_lambda_function" "realtime_alerts" {
  function_name    = "${local.project}-realtime-alerts"
  role             = aws_iam_role.realtime_lambda.arn
  runtime          = "python3.12"
  handler          = "src.lambdas.realtime_alerts.handler.handler"
  filename         = data.archive_file.realtime_lambda.output_path
  source_code_hash = data.archive_file.realtime_lambda.output_base64sha256
  timeout          = local.aws.lambda.timeout_seconds
  memory_size      = local.aws.lambda.memory_mb

  environment {
    variables = {
      # Where the state lives and where alerts go
      EVENTS_SEEN_TABLE    = aws_dynamodb_table.events_seen.name
      PART_STOCK_TABLE     = aws_dynamodb_table.part_stock.name
      STATION_HOURLY_TABLE = aws_dynamodb_table.station_hourly.name
      EVENTS_SEEN_TTL_DAYS = tostring(local.aws.dynamodb.events_seen_ttl_days)
      ALERT_TOPIC_ARN      = aws_sns_topic.alerts.arn

      # Alert thresholds (config/settings.yaml -> alerts)
      REJECT_RATE_BASELINE_PCT      = tostring(local.alerts.reject_rate_baseline_pct)
      REJECT_RATE_SIGMA             = tostring(local.alerts.reject_rate_sigma)
      REJECT_RATE_FLOOR_PCT         = tostring(local.alerts.reject_rate_floor_pct)
      REJECT_RATE_MIN_REJECTS       = tostring(local.alerts.reject_rate_min_rejects)
      STOCK_COVERAGE_THRESHOLD_DAYS = tostring(local.alerts.stock_coverage_threshold_days)
    }
  }

  depends_on = [aws_cloudwatch_log_group.realtime_lambda, aws_iam_role_policy_attachment.realtime_kinesis]
}

# ---------------------------------------------------------------------------
# 6. Event source mapping: how Lambda reads the stream
# ---------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "stream_to_realtime" {
  event_source_arn = aws_kinesis_stream.plant_events.arn
  function_name    = aws_lambda_function.realtime_alerts.arn

  # Start from the oldest record kept: nothing sent before the mapping was ready is missed
  starting_position = "TRIM_HORIZON"

  # Up to 100 records per invocation, waiting at most 5 s to fill a batch
  batch_size                         = local.aws.lambda.batch_size
  maximum_batching_window_in_seconds = local.aws.lambda.max_batching_window_seconds

  # The handler reports exactly which records failed; only those (and later ones) are retried
  function_response_types = ["ReportBatchItemFailures"]

  # On repeated failure: split the batch to isolate the bad record, retry 3 times, then park it
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.realtime_dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.realtime_access]
}
