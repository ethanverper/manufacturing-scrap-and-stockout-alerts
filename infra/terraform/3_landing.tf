# =============================================================================
# 3_landing.tf — Data Firehose: keeps a copy of every scan in the lake, with no code.
#
# Firehose is the stream's second consumer, independent of the Lambda: it reads the same records,
# buffers them (1 MB or 60 s), compresses them and writes files to S3 raw/plant_events/dt=.../
# The Glue job (4_batch.tf) reads those files every night (docs/architecture.md, ADR-006).
# =============================================================================

# ---------------------------------------------------------------------------
# 1. Logs for delivery errors (kept 1 day; created here so destroy removes them too)
# ---------------------------------------------------------------------------

# The log group Firehose writes delivery problems to (e.g. no permission on the bucket)
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.project}-landing"
  retention_in_days = local.aws.firehose.log_retention_days
}

# The log stream inside it, for the S3 delivery
resource "aws_cloudwatch_log_stream" "firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# ---------------------------------------------------------------------------
# 2. IAM: Firehose may read the stream and write only to raw/ and firehose-errors/
# ---------------------------------------------------------------------------

# The role Firehose assumes. The ExternalId condition stops another account's Firehose from using it.
resource "aws_iam_role" "firehose" {
  name = "${local.project}-firehose"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = data.aws_caller_identity.current.account_id } }
    }]
  })
}

# Least privilege: one stream to read, two prefixes to write, one log group
resource "aws_iam_role_policy" "firehose" {
  name = "landing"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Read the scanner stream (the actions a Kinesis consumer needs)
        Sid      = "ReadStream"
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = aws_kinesis_stream.plant_events.arn
      },
      {
        # Find the bucket and resume multipart uploads of large files
        Sid      = "LocateBucket"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        # Write files, only under the landing prefixes
        Sid      = "WriteRawFiles"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:GetObject"]
        Resource = ["${aws_s3_bucket.lake.arn}/raw/plant_events/*", "${aws_s3_bucket.lake.arn}/firehose-errors/*"]
      },
      {
        # Report delivery errors
        Sid      = "WriteErrorLogs"
        Effect   = "Allow"
        Action   = "logs:PutLogEvents"
        Resource = "${aws_cloudwatch_log_group.firehose.arn}:*"
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# 3. The delivery stream
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "landing" {
  name        = "${local.project}-landing"
  destination = "extended_s3"

  # Source: the same Kinesis stream the Lambda reads
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.plant_events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.lake.arn

    # Hive-style folders by arrival date (UTC), the same layout history.py writes, so Athena and
    # Spark read history and live data the same way. The producer already ends each record with
    # "\n", so the files are JSON lines.
    prefix = "raw/plant_events/dt=!{timestamp:yyyy-MM-dd}/"

    # Records Firehose can't deliver go here, by error type, instead of being lost
    error_output_prefix = "firehose-errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    # Write a file every 1 MB or 60 s, whichever comes first (config: aws.firehose)
    buffering_size     = local.aws.firehose.buffer_size_mb
    buffering_interval = local.aws.firehose.buffer_interval_seconds

    # GZIP, with the extension Spark needs to know the files are compressed
    compression_format = "GZIP"
    file_extension     = ".json.gz"

    # Delivery problems go to the log group above
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3.name
    }
  }

  # The role must be able to read the stream before Firehose starts
  depends_on = [aws_iam_role_policy.firehose]
}
