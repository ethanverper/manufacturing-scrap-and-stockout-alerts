# Runbook

| Alert / symptom | What it means | First actions |
|---|---|---|
| | | |

## Teardown
1. `make evidence PHASE=batch` (last proof while the resources still exist)
2. `make destroy`: all 41 resources
3. `make clean-glue-logs`: the log groups Glue created on its own
4. `make leftovers`: must end with `✅ Nothing billable left.` (a check that can't run counts as a failure)
5. `make evidence PHASE=destroyed`
6. The next day: `make cost-daily`, and record the actual cost in `docs/evidence/deploy-log.md`
