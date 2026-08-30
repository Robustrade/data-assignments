PYTHON    ?= python
WORKSPACE ?= .work/demo

.DEFAULT_GOAL := help

.PHONY: help install demo ingest build status restore fixture \
        stop-the-line crash-recovery \
        checks contracts quality catalog lint format test clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

install:  ## Install runtime and validation dependencies
	$(PYTHON) -m pip install -r requirements.txt

# ── the happy path ──────────────────────────────────────────────────────────
demo:  ## Full run from a clean state: seed, workload, ingest, build, reconcile
	$(PYTHON) -m cdclake --workspace $(WORKSPACE) demo

ingest:  ## Drain the source into the lake
	$(PYTHON) -m cdclake --workspace $(WORKSPACE) ingest

build:  ## Rebuild the warehouse and run every in-warehouse assertion
	$(PYTHON) -m cdclake --workspace $(WORKSPACE) build

status:  ## Checkpoint, backlog, orphan files and pipeline state
	$(PYTHON) -m cdclake --workspace $(WORKSPACE) status

restore:  ## Point-in-time restore. ENTITY=customer_order VERSION=39
	$(PYTHON) -m cdclake --workspace $(WORKSPACE) restore \
	  --entity $(ENTITY) --at-version $(VERSION)

fixture:  ## Refresh warehouse/fixtures/lake_cdc_events.ndjson (used by CI)
	$(PYTHON) -m cdclake --workspace .work/fixture fixture

# ── the non-happy paths, which are the interesting ones ─────────────────────
stop-the-line:  ## Break the source schema and watch ingestion halt and recover
	$(PYTHON) scripts/demo_stop_the_line.py $(MUTATION)

crash-recovery:  ## Kill a batch mid-write and prove nothing is lost
	$(PYTHON) scripts/demo_crash_recovery.py

# ── the same checks CI runs ─────────────────────────────────────────────────
checks: lint contracts quality catalog test  ## Everything CI runs, in CI's order

contracts:  ## CI job: schema-contract-check
	$(PYTHON) scripts/check_schema_contracts.py

quality:  ## CI job: data-quality-check
	$(PYTHON) scripts/run_data_quality_checks.py

catalog:  ## CI job: catalog-check
	$(PYTHON) scripts/validate_catalog.py

lint:  ## CI job: lint-python-or-sql
	ruff check .
	black --check .
	sqlfluff lint .

format:  ## Apply formatting and safe lint fixes
	black .
	ruff check . --fix

test:  ## CI job: test-pipeline
	$(PYTHON) -m pytest -q

clean:  ## Remove every runtime artefact (everything here is reproducible)
	rm -rf .work target logs .pytest_cache .ruff_cache
