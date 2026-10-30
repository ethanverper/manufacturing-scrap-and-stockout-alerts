#!/bin/bash
# =============================================================================
# scripts/cost.sh — what this project actually cost, from Cost Explorer (read-only, $0.01 per call).
#
# Two views:
#   cost.sh <project> [start]        cost by service, filtered by the Project tag. Needs the tag
#                                    activated once in Billing > Cost allocation tags (it appears
#                                    only after tagged resources exist; data shows ~24 h later).
#   cost.sh --daily [start]          cost by service and day for the whole account, no tag needed.
#                                    This account spends ~$0 otherwise, so the deploy day IS the
#                                    project's cost. Cost Explorer lags up to 24 h: check the day after.
# =============================================================================
set -euo pipefail

# Default window: the last 7 days, through tomorrow (the end date is exclusive)
default_start() { date -u -v-7d +%Y-%m-%d 2>/dev/null || date -u -d '7 days ago' +%Y-%m-%d; }
END="$(date -u -v+1d +%Y-%m-%d 2>/dev/null || date -u -d 'tomorrow' +%Y-%m-%d)"

# Daily view: every service with a non-zero cost, per day
if [[ "${1:-}" == "--daily" ]]; then
  START="${2:-$(default_start)}"
  aws ce get-cost-and-usage \
    --time-period "Start=$START,End=$END" \
    --granularity DAILY \
    --metrics UnblendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --query 'ResultsByTime[].{day:TimePeriod.Start, costs:Groups[?Metrics.UnblendedCost.Amount!=`"0"`].[Keys[0], Metrics.UnblendedCost.Amount]}' \
    --output json
  exit 0
fi

# Tag view: this project only, by service
PROJECT="${1:?usage: cost.sh <project> [start]  |  cost.sh --daily [start]}"
START="${2:-$(default_start)}"
aws ce get-cost-and-usage \
  --time-period "Start=$START,End=$END" \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --filter "{\"Tags\":{\"Key\":\"Project\",\"Values\":[\"$PROJECT\"]}}" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query 'ResultsByTime[].Groups[].[Keys[0], Metrics.UnblendedCost.Amount]' --output table
