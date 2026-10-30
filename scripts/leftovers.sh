#!/bin/bash
# =============================================================================
# scripts/leftovers.sh — read-only check after `make destroy`: is anything still alive (and billing)?
#
# Three independent nets, so one blind spot can't hide a resource:
#   1. the Project tag      everything Terraform created carries Project=<name> (default_tags)
#   2. the project's name   every resource name starts with <name>, checked service by service
#   3. hourly billers       services that cost money just by existing, whatever their name
# Plus one warning (not a failure): log groups Glue creates on its own, which destroy can't remove
# (scripts/clean_glue_logs.sh deletes the ones created during the deploy).
#
# Usage: scripts/leftovers.sh <project-name>      exit code 1 if anything is found
# =============================================================================
set -uo pipefail
PROJECT="${1:?usage: leftovers.sh <project-name>}"
DATABASE="${PROJECT//-/_}"   # the Athena database name: hyphens become underscores
REGION="${AWS_REGION:-us-east-1}"
found=0

section() { echo; echo "== $1"; }

# check "<label>" <command...>: print the command's output only if it found something.
# A command that fails (no permission, service unavailable) counts as "found": a check that could
# not run must never read as "nothing left".
check() {
  local label="$1"; shift
  local out status
  out=$("$@" 2>&1); status=$?
  out=$(echo "$out" | sed '/^[[:space:]]*$/d')
  if [[ $status -ne 0 ]]; then
    echo "  ❓ $label: could not check ($(echo "$out" | tail -1))"; found=1
  elif [[ -n "$out" && "$out" != "None" && "$out" != "[]" ]]; then
    echo "  ⚠️  $label:"; echo "$out" | sed 's/^/      /'; found=1
  else
    echo "  ✓ $label: none"
  fi
}

# Glue runs still in progress. "Job not found" is the expected answer after destroy (nothing can run).
glue_runs_in_progress() {
  local out
  out=$(aws glue get-job-runs --region "$REGION" --job-name "$PROJECT-daily-kpis" \
    --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text 2>&1) && { echo "$out"; return 0; }
  echo "$out" | grep -q EntityNotFoundException && return 0
  echo "$out"; return 1
}

section "1. Resources still tagged Project=$PROJECT"
check "tagged resources" aws resourcegroupstaggingapi get-resources --region "$REGION" \
  --tag-filters "Key=Project,Values=$PROJECT" --query 'ResourceTagMappingList[].ResourceARN' --output text

section "2. Resources whose name starts with $PROJECT"
check "S3 buckets"         aws s3api list-buckets --query "Buckets[?starts_with(Name, '$PROJECT')].Name" --output text
check "Kinesis streams"    aws kinesis list-streams --region "$REGION" --query "StreamNames[?starts_with(@, '$PROJECT')]" --output text
check "Firehose streams"   aws firehose list-delivery-streams --region "$REGION" --query "DeliveryStreamNames[?starts_with(@, '$PROJECT')]" --output text
check "Lambda functions"   aws lambda list-functions --region "$REGION" --query "Functions[?starts_with(FunctionName, '$PROJECT')].FunctionName" --output text
check "DynamoDB tables"    aws dynamodb list-tables --region "$REGION" --query "TableNames[?starts_with(@, '$PROJECT')]" --output text
check "SNS topics"         aws sns list-topics --region "$REGION" --query "Topics[?contains(TopicArn, ':$PROJECT')].TopicArn" --output text
check "SQS queues"         aws sqs list-queues --region "$REGION" --queue-name-prefix "$PROJECT" --query 'QueueUrls' --output text
check "Glue jobs"          aws glue list-jobs --region "$REGION" --query "JobNames[?starts_with(@, '$PROJECT')]" --output text
check "Glue triggers"      aws glue list-triggers --region "$REGION" --query "TriggerNames[?starts_with(@, '$PROJECT')]" --output text
check "Glue databases"     aws glue get-databases --region "$REGION" --query "DatabaseList[?Name=='$DATABASE'].Name" --output text
check "Athena workgroups"  aws athena list-work-groups --region "$REGION" --query "WorkGroups[?Name=='$PROJECT'].Name" --output text
check "IAM roles"          aws iam list-roles --query "Roles[?starts_with(RoleName, '$PROJECT')].RoleName" --output text
check "Log groups"         aws logs describe-log-groups --region "$REGION" --query "logGroups[?contains(logGroupName, '$PROJECT')].logGroupName" --output text

section "3. Services that bill by the hour (any project, this region)"
check "Kinesis streams (any)"     aws kinesis list-streams --region "$REGION" --query 'StreamNames' --output text
check "Firehose streams (any)"    aws firehose list-delivery-streams --region "$REGION" --query 'DeliveryStreamNames' --output text
check "Flink applications"        aws kinesisanalyticsv2 list-applications --region "$REGION" --query 'ApplicationSummaries[].ApplicationName' --output text
check "Glue triggers (activated)" aws glue get-triggers --region "$REGION" --query "Triggers[?State=='ACTIVATED'].Name" --output text
check "Glue jobs running"         glue_runs_in_progress
check "Redshift Serverless"       aws redshift-serverless list-workgroups --region "$REGION" --query 'workgroups[].workgroupName' --output text
check "EMR Serverless apps"       aws emr-serverless list-applications --region "$REGION" --query "applications[?state!='TERMINATED'].name" --output text
check "NAT gateways"              aws ec2 describe-nat-gateways --region "$REGION" --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId' --output text
check "Interface VPC endpoints"   aws ec2 describe-vpc-endpoints --region "$REGION" --filters Name=vpc-endpoint-type,Values=Interface --query 'VpcEndpoints[].VpcEndpointId' --output text
check "SageMaker endpoints"       aws sagemaker list-endpoints --region "$REGION" --query 'Endpoints[].EndpointName' --output text
check "OpenSearch Serverless"     aws opensearchserverless list-collections --region "$REGION" --query 'collectionSummaries[].name' --output text

# Glue's own log groups: not created by Terraform, a few KB, cost ~$0, but reported so nothing is forgotten
section "Note: log groups Glue created on its own (not a failure; remove with make clean-glue-logs)"
aws logs describe-log-groups --region "$REGION" --log-group-name-prefix /aws-glue \
  --query 'logGroups[].[logGroupName, storedBytes]' --output text 2>/dev/null | sed 's/^/      /'

echo
if [[ $found -eq 0 ]]; then echo "✅ Nothing billable left."; else echo "❌ Something is still there. Destroy it before you leave."; exit 1; fi
