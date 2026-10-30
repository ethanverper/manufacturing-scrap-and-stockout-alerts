#!/bin/bash
# =============================================================================
# scripts/clean_glue_logs.sh — delete the CloudWatch log groups Glue created during this deploy.
#
# Why this exists: Glue writes its logs to shared groups (/aws-glue/jobs/output, /error, ...) that it
# creates on the first run. Terraform didn't create them, so `make destroy` can't remove them.
# Only groups created AFTER the deploy started are deleted (the time `make deploy` saved in
# infra/terraform/build/deploy_started_at), so logs of older, unrelated jobs are never touched.
#
# Usage: scripts/clean_glue_logs.sh      (after make destroy)
# =============================================================================
set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
MARKER="infra/terraform/build/deploy_started_at"

# Without the deploy start time we can't tell our groups from older ones: stop
if [[ ! -f "$MARKER" ]]; then
  echo "No $MARKER (written by make deploy): not deleting anything."; exit 1
fi
STARTED_MS="$(cat "$MARKER")"

# Glue log groups created after the deploy started
GROUPS=$(aws logs describe-log-groups --region "$REGION" --log-group-name-prefix /aws-glue \
  --query "logGroups[?creationTime >= \`$STARTED_MS\`].logGroupName" --output text)

# Delete each one, saying what was deleted
for group in $GROUPS; do
  aws logs delete-log-group --region "$REGION" --log-group-name "$group"
  echo "deleted $group"
done
[[ -z "$GROUPS" ]] && echo "No Glue log groups created during this deploy."
exit 0
