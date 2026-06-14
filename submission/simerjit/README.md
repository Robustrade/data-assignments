# CDC Lakehouse Reliability Assignment

Databricks, PySpark, and Delta Lake solution for the Robustrade data engineering assignment.

## Stack

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Volumes
- YAML metadata/configuration

## Run Order

Run the notebooks in this order:

1. `00_setup_and_source_model`
2. `01_generate_cdc_events`
3. `02_bronze_ingest_delta`
4. `03_apply_silver_merges`
5. `06_cdc_correctness_tests`
6. `05_quality_checks`
7. `08_catalog_metadata`
8. `04_restore_with_delta_time_travel`
9. `09_final_summary_or_pr_notes`

## What The Project Demonstrates

- Source schema design for a wallet/payments domain.
- Simulated CDC event contract.
- Insert, update, and delete CDC events.
- Bronze Delta append-only history.
- Silver Delta current-state tables.
- Schema-safe stopping before incompatible writes.
- CDC correctness and idempotency checks.
- Validation parity for warehouse tables.
- Catalog metadata.
- Delta time travel restore.

## Important Tables

- `robustrade_dev.bronze.cdc_events`
- `robustrade_dev.silver.customers_current`
- `robustrade_dev.silver.wallet_accounts_current`
- `robustrade_dev.silver.merchants_current`
- `robustrade_dev.silver.payment_transactions_current`
- `robustrade_dev.silver.payment_attempts_current`

## Supporting Files

- `APPROACH.md`: initial understanding and implementation approach.
- `PR_DESCRIPTION.md`: PR-style summary mapped to assignment expectations.
- `catalog.yml`: dataset catalog metadata.
- `schema_contracts.yml`: expected source payload contracts.
- `job_config.yml`: notebook task order and dependencies.
- `sql/source_schema.sql`: source model DDL.

