# PR: CDC Lakehouse Reliability Assignment

## Summary

This PR implements a CDC lakehouse reliability pipeline using Databricks, PySpark, and Delta Lake. It simulates a wallet/payments source system, writes CDC events to a Databricks Volume, ingests them into an append-only Bronze Delta table, applies schema-safe Delta merges into Silver current-state tables, runs validation checks, documents catalog metadata, and demonstrates Delta time travel restore.

## Source Schema Design

The source model has five tables:

- `customers`
- `wallet_accounts`
- `merchants`
- `payment_transactions`
- `payment_attempts`

`customers`, `wallet_accounts`, `merchants`, and `payment_transactions` are strong entities. `payment_attempts` is a weak entity because each attempt depends on a parent payment transaction.

The source design includes primary keys, foreign keys, indexes, timestamps, nullable optional fields, currency fields, status domains, non-negative wallet balance checks, and positive payment amount checks.

## CDC Strategy

CDC is simulated as JSON files in a Databricks Volume. Each event includes `source_table`, `cdc_operation`, `cdc_sequence`, `source_commit_ts`, `schema_version`, and `payload_json`.

The sample stream contains inserts, an update, and a delete. In production, this simulated stream would be replaced by database logs, Debezium, Kafka, Fivetran, or another CDC connector.

## Lake And Warehouse Modeling

The lake layer is `robustrade_dev.bronze.cdc_events`. It is append-only and retains full CDC history.

The warehouse layer contains Silver current-state tables. These tables are built with Delta `MERGE`, ordered by `source_commit_ts` and `cdc_sequence`, and retain only the latest non-deleted row per key.

## Schema Change Safety

Before writing to Silver, each CDC payload is parsed using an explicit schema and checked against a contract. Missing required fields, data type changes, or required fields becoming nullable stop the pipeline before downstream mutation.

## Validation Parity

Warehouse checks mirror source business rules:

- primary key uniqueness
- referential integrity
- non-negative wallet balances
- positive payment amounts
- accepted transaction statuses
- duplicate CDC sequence detection

## Catalog Exposure

Catalog metadata is provided in `catalog.yml` and the `08_catalog_metadata` notebook. It documents dataset name, layer, platform, owner, description, consumers, primary key, and update cadence.

## Historical Recovery

Silver tables can be rebuilt from Bronze. The restore notebook also demonstrates Delta Lake time travel with `DESCRIBE HISTORY` and `RESTORE TABLE`.

## Responsible AI Usage

AI assistance was used to help structure the notebooks, debug Databricks errors, and refine explanations. I personally ran the notebooks in Databricks, reviewed the outputs, corrected errors, and validated the final pipeline behavior.

## Testing And Validation

Completed validations:

- 7 CDC events generated.
- 7 events loaded into Bronze Delta.
- Schema safe-stop test failed correctly for an unsafe type change.
- Silver merges created current-state tables.
- `payment_transactions_current` showed transaction `t_001` as `SETTLED`.
- CDC correctness checks passed.
- Rerunning the Silver merge did not create duplicate rows.
- Quality checks passed.
- Catalog metadata coverage passed.
- Delta restore returned a corrupted transaction status back to `SETTLED`.

