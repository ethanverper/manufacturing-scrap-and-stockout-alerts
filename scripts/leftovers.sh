#!/bin/bash
# Read-only check after `make destroy`: is anything still alive (and billing)?
# Usage: scripts/leftovers.sh <project-tag>
set -uo pipefail
PROJECT="${1:?usage: leftovers.sh <project-tag>}"
REGION="${AWS_REGION:-us-east-1}"
found=0

section() { echo; echo "== $1"; }
check() { # check "<label>" <command...>  -> prints output only if non-empty
  local label="$1"; shift
  local out
  out=$("$@" 2>/dev/null | sed '/^\s*$/d')
  if [[ -n "$out" && "$out" != "None" && "$out" != "[]" ]]; then
    echo "  ⚠️  $label:"; echo "$out" | sed 's/^/      /'; found=1
  fi
}

section "Resources still tagged Project=$PROJECT"
check "tagged resources" aws resourcegroupstaggingapi get-resources --region "$REGION" \
  --tag-filters "Key=Project,Values=$PROJECT" --query 'ResourceTagMappingList[].ResourceARN' --output text

section "Services that bill by the hour (any project, this region)"
check "Kinesis streams"            aws kinesis list-streams --region "$REGION" --query 'StreamNames' --output text
check "Firehose streams"           aws firehose list-delivery-streams --region "$REGION" --query 'DeliveryStreamNames' --output text
check "Flink applications"         aws kinesisanalyticsv2 list-applications --region "$REGION" --query 'ApplicationSummaries[].ApplicationName' --output text
check "Glue triggers (activated)"  aws glue get-triggers --region "$REGION" --query "Triggers[?State=='ACTIVATED'].Name" --output text
check "Redshift Serverless"        aws redshift-serverless list-workgroups --region "$REGION" --query 'workgroups[].workgroupName' --output text
check "EMR Serverless apps"        aws emr-serverless list-applications --region "$REGION" --query "applications[?state!='TERMINATED'].name" --output text
check "NAT gateways"               aws ec2 describe-nat-gateways --region "$REGION" --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId' --output text
check "Interface VPC endpoints"    aws ec2 describe-vpc-endpoints --region "$REGION" --filters Name=vpc-endpoint-type,Values=Interface --query 'VpcEndpoints[].VpcEndpointId' --output text
check "SageMaker endpoints"        aws sagemaker list-endpoints --region "$REGION" --query 'Endpoints[].EndpointName' --output text
check "OpenSearch Serverless"      aws opensearchserverless list-collections --region "$REGION" --query 'collectionSummaries[].name' --output text

echo
if [[ $found -eq 0 ]]; then echo "✅ Nothing billable left."; else echo "❌ Something is still running. Destroy it before you leave."; exit 1; fi
