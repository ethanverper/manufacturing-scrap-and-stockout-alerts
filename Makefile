PROJECT ?= $(shell basename $(CURDIR))
TF := terraform -chdir=infra/terraform
PYTHON ?= python3

.PHONY: help install test lint fmt generate live-dry deploy demo destroy leftovers cost

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

deploy: ## Create the stack (read the cost estimate in docs/spec.md first)
	$(TF) init -input=false
	$(TF) apply -var="project=$(PROJECT)"

demo: ## Send data / run jobs (define per project)
	@echo "Define the demo steps for this project"

destroy: ## Delete everything
	$(TF) destroy -var="project=$(PROJECT)"

leftovers: ## Check nothing billable is still running
	scripts/leftovers.sh $(PROJECT)

cost: ## Actual cost of this project (Cost Explorer, tag Project)
	scripts/cost.sh $(PROJECT)
