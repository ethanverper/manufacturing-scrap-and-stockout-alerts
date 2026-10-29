# 0_main.tf: provider, shared settings and the data lake bucket.
# Add one numbered file per pipeline stage: 1_ingestion.tf, 2_processing.tf, 3_alerts.tf, 4_analytics.tf ...

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

variable "project" {
  description = "Repo name. Tags every resource so its cost can be tracked."
  type        = string
  default     = "manufacturing-scrap-and-stockout-alerts"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

provider "aws" {
  region = var.region

  # Every resource gets these tags -> Cost Explorer can report this project's exact cost.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

# Who is running Terraform: used to build a bucket name that is unique per AWS account
data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.project}-${data.aws_caller_identity.current.account_id}" # unique per account
}

# ---------- Data lake bucket: raw/ curated/ gold/ scripts/ athena-results/ ----------

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
      days = 7
    }
  }
}

output "bucket" {
  value = aws_s3_bucket.lake.bucket
}
