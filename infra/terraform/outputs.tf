# =============================================================================
# outputs.tf — names that the Makefile and scripts need after `terraform apply`.
# Read one with: terraform -chdir=infra/terraform output -raw <name>
# =============================================================================

# Where `make upload-history` and Firehose put data
output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

# Where `make stream` sends the live session
output "stream_name" {
  value = aws_kinesis_stream.plant_events.name
}

# Where `make seed` loads the opening stock
output "part_stock_table" {
  value = aws_dynamodb_table.part_stock.name
}

# The alerts topic (to inspect subscriptions in the console or with the CLI)
output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

# Who is subscribed, and to what (email -> filter policy). Lets you check the YAML was applied.
output "alert_subscriptions" {
  value = { for key, subscription in aws_sns_topic_subscription.email : subscription.endpoint => subscription.filter_policy }
}

# Failed batches waiting to be replayed (should stay empty)
output "realtime_dlq_url" {
  value = aws_sqs_queue.realtime_dlq.url
}
