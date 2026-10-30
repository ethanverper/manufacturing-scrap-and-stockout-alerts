#!/bin/bash
# =============================================================================
# scripts/evidence.sh — save proof of a real deploy into docs/evidence/<phase>/, safe to publish.
#
# Each phase captures read-only AWS CLI output (nothing is created or changed):
#   deployed   right after `make deploy`: every resource exists and is configured as designed
#   streamed   after `make stream`: events processed, alerts sent, nothing failed, files landed
#   batch      after `make kpis`: the Glue run, its reconciliation line, the gold tables in S3
#   destroyed  after `make destroy`: Terraform state is empty and nothing billable is left
#
# Redaction: the repo is public, so the AWS account id and every email address are replaced
# with <account-id> and <email> before anything is written.
#
# Usage: scripts/evidence.sh <deployed|streamed|batch|destroyed>
# =============================================================================
set -uo pipefail

PHASE="${1:?usage: evidence.sh <deployed|streamed|batch|destroyed>}"
TF="terraform -chdir=infra/terraform"
REGION="${AWS_REGION:-us-east-1}"
OUT="docs/evidence/$PHASE"
mkdir -p "$OUT"

# The account id, read once, so it can be blanked out of every file
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Replace the account id and any email address with placeholders
redact() {
  sed -E -e "s/$ACCOUNT_ID/<account-id>/g" -e 's/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/<email>/g'
}

# capture <file> <command...>: run a read-only command, redact its output, save it, show a one-line status
capture() {
  local file="$1"; shift
  {
    echo "\$ $*" | redact
    echo "# captured $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    "$@" 2>&1
  } | redact > "$OUT/$file"
  echo "  saved $OUT/$file"
}

# Names of the deployed resources, from Terraform's outputs (empty after destroy)
out() { $TF output -raw "$1" 2>/dev/null; }
PROJECT=$(grep -m1 'name:' config/settings.yaml | awk '{print $2}')

echo "== Evidence: $PHASE"
case "$PHASE" in
  deployed)
    # Everything Terraform created, and the same list as AWS sees it through the Project tag
    capture terraform-state.txt $TF state list
    capture tagged-resources.txt aws resourcegroupstaggingapi get-resources --region "$REGION" \
      --tag-filters "Key=Project,Values=$PROJECT" --query 'ResourceTagMappingList[].ResourceARN' --output text
    # Ingestion and real-time path, configured as designed
    capture kinesis-stream.json aws kinesis describe-stream-summary --stream-name "$(out stream_name)" \
      --query 'StreamDescriptionSummary.{status:StreamStatus,shards:OpenShardCount,retention_hours:RetentionPeriodHours,encryption:EncryptionType,mode:StreamModeDetails.StreamMode}'
    capture lambda-config.json aws lambda get-function-configuration --function-name "$PROJECT-realtime-alerts" \
      --query '{runtime:Runtime,memory_mb:MemorySize,timeout_s:Timeout,state:State,environment:Environment.Variables}'
    capture lambda-stream-mapping.json aws lambda list-event-source-mappings --function-name "$PROJECT-realtime-alerts" \
      --query 'EventSourceMappings[].{state:State,batch_size:BatchSize,bisect_on_error:BisectBatchOnFunctionError,retries:MaximumRetryAttempts,partial_failures:FunctionResponseTypes}'
    capture dynamodb-tables.txt bash -c "for t in events-seen part-stock station-hourly; do aws dynamodb describe-table --table-name $PROJECT-\$t --query 'Table.[TableName,TableStatus,BillingModeSummary.BillingMode]' --output text; done"
    capture sns-subscriptions.json aws sns list-subscriptions-by-topic --topic-arn "$(out alerts_topic_arn)" \
      --query 'Subscriptions[].{endpoint:Endpoint,protocol:Protocol,status:SubscriptionArn}'
    capture sns-filter-policies.json $TF output -json alert_subscriptions
    # Landing, batch and analytics
    capture firehose.json aws firehose describe-delivery-stream --delivery-stream-name "$(out firehose_name)" \
      --query 'DeliveryStreamDescription.{status:DeliveryStreamStatus,source:Source.KinesisStreamSourceDescription.KinesisStreamARN,prefix:Destinations[0].ExtendedS3DestinationDescription.Prefix,compression:Destinations[0].ExtendedS3DestinationDescription.CompressionFormat,buffering:Destinations[0].ExtendedS3DestinationDescription.BufferingHints}'
    capture glue-job.json aws glue get-job --job-name "$(out glue_job_name)" \
      --query 'Job.{glue_version:GlueVersion,worker_type:WorkerType,workers:NumberOfWorkers,timeout_min:Timeout,max_retries:MaxRetries}'
    capture glue-trigger.json aws glue get-trigger --name "$PROJECT-nightly" --query 'Trigger.{schedule:Schedule,state:State}'
    capture athena-tables.txt aws glue get-tables --database-name "$(out athena_database)" --query 'TableList[].Name' --output text
    capture athena-workgroup.json aws athena get-work-group --work-group "$(out athena_workgroup)" \
      --query 'WorkGroup.Configuration.{scan_limit_bytes:BytesScannedCutoffPerQuery,enforced:EnforceWorkGroupConfiguration,results:ResultConfiguration.OutputLocation}'
    capture s3-security.txt bash -c "aws s3api get-public-access-block --bucket $(out bucket) --output text; aws s3api get-bucket-encryption --bucket $(out bucket) --query 'ServerSideEncryptionConfiguration.Rules[].ApplyServerSideEncryptionByDefault' --output text"
    ;;

  streamed)
    # The Lambda's own log lines for every alert it sent (subject and numbers)
    capture lambda-alerts-sent.txt aws logs filter-log-events --log-group-name "/aws/lambda/$PROJECT-realtime-alerts" \
      --filter-pattern '"Alert sent"' --query 'events[].message' --output text
    # Invocations and errors over the last 2 hours, from CloudWatch metrics
    capture lambda-metrics.txt bash -c "for m in Invocations Errors Throttles; do echo -n \"\$m: \"; aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name \$m --dimensions Name=FunctionName,Value=$PROJECT-realtime-alerts --start-time \$(date -u -v-2H +%Y-%m-%dT%H:%M:%SZ) --end-time \$(date -u +%Y-%m-%dT%H:%M:%SZ) --period 7200 --statistics Sum --query 'Datapoints[0].Sum' --output text; done"
    # Nothing waiting in the dead-letter queue
    capture dlq-depth.txt aws sqs get-queue-attributes --queue-url "$(out realtime_dlq_url)" \
      --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text
    # Live stock in DynamoDB, lowest first
    capture dynamodb-part-stock.txt bash -c "aws dynamodb scan --table-name $(out part_stock_table) --query 'Items[].[plant_part.S,on_hand.N,planned_daily_usage.N]' --output text | awk '{printf \"%-28s on_hand=%8s  coverage_days=%.3f\n\", \$1, \$2, \$2/\$3}' | sort -t= -k3 -n"
    # Files Firehose landed (and the history upload), per day
    capture s3-raw-files.txt aws s3 ls "s3://$(out bucket)/raw/plant_events/" --recursive --summarize
    ;;

  batch)
    # The most recent Glue run: state, duration and capacity
    capture glue-job-run.json aws glue get-job-runs --job-name "$(out glue_job_name)" --max-items 1 \
      --query 'JobRuns[0].{state:JobRunState,started:StartedOn,seconds:ExecutionTime,workers:NumberOfWorkers,worker_type:WorkerType,error:ErrorMessage}'
    # The job's own summary line: record reconciliation and rows per table. Python logging writes to
    # stderr, which Glue sends to the "error" group; look in both so nothing depends on that detail.
    capture glue-summary-log.txt bash -c "for group in /aws-glue/jobs/output /aws-glue/jobs/error; do aws logs filter-log-events --log-group-name \$group --filter-pattern '\"Daily KPIs written\"' --query 'events[-1].message' --output text 2>/dev/null; done | grep -v '^None$'"
    # What it wrote
    capture s3-gold-files.txt aws s3 ls "s3://$(out bucket)/gold/" --recursive --summarize
    ;;

  destroyed)
    # Terraform has nothing left to manage, and the cleanup check finds nothing
    capture terraform-state.txt bash -c "$TF state list; echo \"resources in state: \$($TF state list 2>/dev/null | wc -l | tr -d ' ')\""
    capture leftovers.txt scripts/leftovers.sh "$PROJECT"
    ;;

  *)
    echo "unknown phase: $PHASE (deployed|streamed|batch|destroyed)"; exit 2 ;;
esac
