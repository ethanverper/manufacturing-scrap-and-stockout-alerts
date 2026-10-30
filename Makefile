PROJECT ?= $(shell basename $(CURDIR))  # same as config/settings.yaml -> project.name
TF := terraform -chdir=infra/terraform
PYTHON ?= python3

.PHONY: help install test lint fmt generate live-dry kpis-local charts-local seed deploy upload-history stream kpis charts destroy leftovers cost

help: ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Install dev tools
	$(PYTHON) -m pip install -r requirements-dev.txt

test: ## Unit tests (no AWS)
	$(PYTHON) -m pytest -q

lint: ## Python lint + Terraform format check
	ruff check .
	$(TF) fmt -check -recursive

fmt: ## Format Python and Terraform
	ruff format .
	$(TF) fmt -recursive

generate: ## Simulate 7 days of plant history + master data into data/generated (local, no AWS)
	$(PYTHON) -m src.generators.history --out data/generated

live-dry: ## Write the live session to data/generated/live_events.jsonl instead of Kinesis
	$(PYTHON) -m src.generators.live --dry-run

kpis-local: ## Run the Glue KPI job on data/generated with a local Spark, into data/lake-local (no AWS)
	$(PYTHON) -m scripts.kpis_local --input data/generated --output data/lake-local

charts-local: ## Run the sql/ queries on data/lake-local with Spark and draw the charts into data/charts (no AWS)
	$(PYTHON) -m scripts.make_charts --engine local --lake data/lake-local --out data/charts

seed: ## Load the end-of-history stock into DynamoDB (after deploy, before the live session)
	$(PYTHON) -m scripts.seed_stock --table $$($(TF) output -raw part_stock_table)

deploy: ## Create the stack (read the cost estimate in docs/spec.md first)
	$(TF) init -input=false
	$(TF) apply

upload-history: ## Upload the generated history and master data to the lake bucket (after deploy)
	aws s3 sync data/generated/raw s3://$$($(TF) output -raw bucket)/raw/
	aws s3 sync data/generated/reference s3://$$($(TF) output -raw bucket)/reference/

stream: ## Stream the live session to Kinesis (after seed); alerts arrive by email within seconds
	$(PYTHON) -m src.generators.live --stream $$($(TF) output -raw stream_name)

kpis: ## Start the Glue KPI job on AWS (~2 DPU x 3 min, about $0.05 a run); prints the run id
	aws glue start-job-run --job-name $$($(TF) output -raw glue_job_name) --query JobRunId --output text

charts: ## Run the sql/ queries on Athena (after make kpis) and save the README charts into docs/images
	$(PYTHON) -m scripts.make_charts --engine athena --out docs/images \
		--workgroup $$($(TF) output -raw athena_workgroup) --database $$($(TF) output -raw athena_database)

destroy: ## Delete everything
	$(TF) destroy

leftovers: ## Check nothing billable is still running
	scripts/leftovers.sh $(PROJECT)

cost: ## Actual cost of this project (Cost Explorer, tag Project)
	scripts/cost.sh $(PROJECT)
