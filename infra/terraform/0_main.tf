# =============================================================================
# 0_main.tf — provider, the central configuration, and the data lake bucket.
#
# The stack is split into one numbered file per pipeline stage, in the order data flows:
#   0_main.tf             provider, settings (from config/*.yaml), S3 bucket
#   1_ingestion.tf        Kinesis Data Streams (the scanners send here)
#   2_realtime.tf         Lambda + DynamoDB + SNS + DLQ (alerts within seconds)
#   3_landing.tf          Firehose -> S3 raw/                          (level 5)
#   4_batch.tf            Glue job + schedule                         (level 5)
#   5_analytics.tf        Glue Catalog tables + Athena workgroup      (level 6)
#   outputs.tf            names other tools need (Makefile, scripts)
# Terraform reads every .tf file in this folder as one configuration; the numbers are for humans.
# =============================================================================

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

# ---------- Central configuration (docs/configuration.md explains every file) ----------

locals {
  # Every tunable value: thresholds, sizing, timings. The same file the Python code reads.
  settings = yamldecode(file("${path.module}/../../config/settings.yaml"))

  # Who receives which alerts. Private file (gitignored); without it, no email subscriptions are created.
  # try() returns [] when the file doesn't exist. Each entry is normalized to the same shape (missing
  # lists become []) because Terraform needs every element of a collection to have the same type.
  subscribers_file = "${path.module}/../../config/subscribers.yaml"
  subscribers = [
    for subscriber in try(yamldecode(file(local.subscribers_file)).subscribers, []) : {
      email  = subscriber.email
      role   = subscriber.role
      alerts = try(subscriber.alerts, [])
      plants = try(subscriber.plants, [])
    }
  ]

  # Short names used everywhere below
  project = local.settings.project.name
  aws     = local.settings.aws
  alerts  = local.settings.alerts

  # Bucket names are global across all AWS accounts: the account id makes ours unique
  bucket_name = "${local.project}-${data.aws_caller_identity.current.account_id}"

  # Repository root, for packaging code from src/
  repo_root = "${path.module}/../.."
}

provider "aws" {
  region = local.settings.project.region

  # Every resource gets these tags, so Cost Explorer can report this project's exact cost (make cost)
  default_tags {
    tags = {
      Project   = local.project
      ManagedBy = "terraform"
    }
  }
}

# Who is running Terraform: used to build a bucket name that is unique per AWS account
data "aws_caller_identity" "current" {}

# ---------- Data lake bucket: raw/ curated/ gold/ reference/ scripts/ athena-results/ ----------

# The single bucket that holds every layer of the lake (one prefix per layer)
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true # demo project: destroy also deletes the data
}

# Nobody outside the account can read the lake: block every form of public access
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Encryption at rest with an AWS-managed key (no $1/month customer-managed KMS key)
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Encryption in transit: reject any request that is not HTTPS
resource "aws_s3_bucket_policy" "tls_only" {
  bucket = aws_s3_bucket.lake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.lake]
}

# Temporary data expires on its own
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = local.aws.athena.results_expiration_days
    }
  }
}
