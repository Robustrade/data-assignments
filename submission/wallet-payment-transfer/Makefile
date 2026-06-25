.PHONY: help install test lint demo demo-time-travel demo-schema-change \
        check-schema check-quality check-catalog ci all

PY ?= python

help:
	@echo "Targets:"
	@echo "  install            - install Python dependencies"
	@echo "  test               - run all pytest tests"
	@echo "  check-schema       - run schema-contract gate"
	@echo "  check-quality      - run data-quality gate"
	@echo "  check-catalog      - run catalog validation gate"
	@echo "  demo               - run end-to-end pipeline demo"
	@echo "  demo-time-travel   - run point-in-time reconstruction demo"
	@echo "  demo-schema-change - run schema drift / stop-the-line demo"
	@echo "  ci                 - run all CI gates (test + 3 checks)"
	@echo "  all                - install, then run ci"

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

check-schema:
	$(PY) -m scripts.check_schema_contracts

check-quality:
	$(PY) -m scripts.run_data_quality_checks

check-catalog:
	$(PY) -m scripts.validate_catalog

demo:
	$(PY) -m scripts.run_pipeline

demo-time-travel:
	$(PY) -m scripts.demo_time_travel

demo-schema-change:
	$(PY) -m scripts.demo_schema_change

ci: test check-schema check-quality check-catalog
	@echo "All CI gates passed."

all: install ci
