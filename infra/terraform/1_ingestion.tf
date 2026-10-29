# =============================================================================
# 1_ingestion.tf — where the scanners send every event: Kinesis Data Streams.
#
# Why a stream and not Firehose alone (docs/decisions.md, ADR-001): two consumers read the same
# events at different speeds (the Lambda in seconds, Firehose in ~1 minute), and the stream keeps
# them for replay after a consumer bug.
# =============================================================================

# The stream. Provisioned with 1 shard (ADR-002): ~10 events/second in the demo, far below the
# 1,000 records/s a shard accepts, and the cheapest option (~$0.015 per hour).
resource "aws_kinesis_stream" "plant_events" {
  name             = "${local.project}-events"
  shard_count      = local.aws.kinesis.shard_count
  retention_period = local.aws.kinesis.retention_hours

  # Encryption at rest with the AWS-managed Kinesis key (no monthly key fee)
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}
