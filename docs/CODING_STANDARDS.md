# Coding standards

Every repository in this portfolio follows the same rules, so any reader (recruiter, engineer, business analyst) can open any file and understand **what it does and why**.

## 1. Language
- **Everything is in English:** code, comments, docstrings, commit messages, file and resource names, logs, alerts, docs.

## 2. Structure and modularity
- **One responsibility per module and per function.** If a function needs "and" to describe it, split it.
- **Small functions** (roughly under 30 lines). Orchestration functions only call other functions; they don't hold business logic.
- **Business logic is pure and testable**: parsing, KPI math and rules live in plain functions with no AWS calls. AWS clients are created in one place (`src/common/aws.py`) and passed in or lazily created, so tests never touch the cloud.
- **No magic numbers or strings.** Thresholds, table names and prefixes live in a config module or environment variables, with names that explain them (`SAFETY_STOCK_UNITS = 50`).
- **Python layout:**
  ```
  src/
    common/        shared helpers: config, logging, AWS clients
    generators/    synthetic data producers
    lambdas/<fn>/  one folder per Lambda: handler.py (thin) + logic modules
    glue_jobs/     one file per job: extract() / transform() / load() + main()
  tests/           mirrors src/, one test file per module
  ```
- **Terraform layout:** numbered files, one per pipeline stage (`0_main.tf`, `1_ingestion.tf`, …), plus `variables.tf` and `outputs.tf`. Resource names follow `${var.project}-<component>`.
- **SQL:** one query per KPI or business question in `sql/`, named `NN_<question>.sql`.

## 3. Comments and documentation
The goal is that the code reads like a guided explanation, not a wall of code.

| Element | Required |
|---|---|
| **Every file** | A header docstring/comment: what this file does, where it sits in the pipeline, inputs and outputs. |
| **Every function** | A docstring: purpose, *why* it exists, args, returns, raises (Google style). |
| **Inside functions** | A comment before every logical step (usually every 1–5 lines), explaining *what* and, above all, *why*. Non-obvious single lines get their own comment. |
| **Every Terraform resource** | A comment above it: what it is, why it is needed, and any cost or security consideration. |
| **Every SQL query** | A header with the business question, then a comment per CTE/step. |
| **Every shell script** | A header (purpose and usage) and a comment per step. |

Comment the **why** over the **what**: `# Drop retries from scanners that lost the network (same event_id)` is better than `# drop duplicates`.

## 4. Code quality
- **Type hints** on every function signature.
- **Logging, not print,** in deployed code (`src/common/logging_utils.py`): structured JSON lines, so CloudWatch Logs Insights can query them.
- **Errors:** validate inputs at the boundary, fail loudly with a clear message, never silently swallow exceptions.
- **Idempotency:** any consumer or job can be re-run without double counting (unique IDs, conditional writes, overwrite-by-partition).
- **Formatting and lint:** `ruff format` + `ruff check` (rules E, F, I, B, UP), `terraform fmt`. CI fails if they don't pass.
- **Tests:** every logic module has unit tests. Tests run offline (`make test`).

## 5. Git
- Small commits with clear messages in the imperative: `Add low-stock alert rule`.
- `main` always passes CI.
