# Approach

## Understanding

The assignment asks for a CDC lakehouse reliability pipeline. The solution should model a realistic transactional source, capture inserts/updates/deletes, preserve raw change history in a lake layer, publish current-state warehouse tables, stop on unsafe schema changes, support restore/replay, expose catalog metadata, and include validations or tests.

## Proposed Solution

I implemented the assignment with Databricks, PySpark, and Delta Lake using a wallet and payments domain.

The source model includes:

- `customers`
- `wallet_accounts`
- `merchants`
- `payment_transactions`
- `payment_attempts`

CDC events are simulated as JSON files in a Databricks Volume. Each event contains:

- `source_table`
- `cdc_operation`
- `cdc_sequence`
- `source_commit_ts`
- `schema_version`
- `payload_json`

## Lakehouse Design

The Bronze lake table is:

- `robustrade_dev.bronze.cdc_events`

It is append-only and keeps every CDC event.

The Silver warehouse tables are:

- `robustrade_dev.silver.customers_current`
- `robustrade_dev.silver.wallet_accounts_current`
- `robustrade_dev.silver.merchants_current`
- `robustrade_dev.silver.payment_transactions_current`
- `robustrade_dev.silver.payment_attempts_current`

Silver tables are maintained with Delta `MERGE` and keep the latest non-deleted row per primary key.

## Reliability Plan

The implementation includes:

- CDC inserts, updates, and deletes.
- Auto Loader ingestion into Bronze Delta.
- Delta `MERGE` for Silver current-state tables.
- Schema contract checks before Silver writes.
- CDC correctness checks for update/delete/idempotency behavior.
- Validation parity checks for business rules.
- Catalog metadata.
- Delta time travel restore demo.

## Recovery Strategy

Bronze Delta is append-only and replayable. Silver tables can be rebuilt from Bronze, and accidental Silver changes can be recovered with Delta Lake history using `DESCRIBE HISTORY` and `RESTORE TABLE`.

## Responsible AI Usage

AI assistance was used to help structure the notebooks, debug Databricks errors, and refine explanations. I personally ran the notebooks in Databricks, reviewed the outputs, corrected errors, and validated the final pipeline behavior.

