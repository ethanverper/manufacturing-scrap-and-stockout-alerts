#!/bin/bash
# Actual cost of this project, from Cost Explorer, filtered by the Project tag.
# One-time setup: Billing > Cost allocation tags > activate "Project". Costs show up ~24h later.
# Usage: scripts/cost.sh <project-tag> [start-date YYYY-MM-DD]
set -euo pipefail
PROJECT="${1:?usage: cost.sh <project-tag> [start-date]}"
START="${2:-$(date -u -v-30d +%Y-%m-%d 2>/dev/null || date -u -d '30 days ago' +%Y-%m-%d)}"
END="$(date -u -v+1d +%Y-%m-%d 2>/dev/null || date -u -d 'tomorrow' +%Y-%m-%d)"

aws ce get-cost-and-usage \
  --time-period "Start=$START,End=$END" \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --filter "{\"Tags\":{\"Key\":\"Project\",\"Values\":[\"$PROJECT\"]}}" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query 'ResultsByTime[].Groups[].[Keys[0], Metrics.UnblendedCost.Amount]' --output table
