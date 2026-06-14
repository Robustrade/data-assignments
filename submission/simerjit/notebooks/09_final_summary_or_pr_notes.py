# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Lakehouse Reliability Assignment
# MAGIC
# MAGIC This project implements a Databricks, PySpark, and Delta Lake CDC lakehouse pipeline for a wallet and payments domain.
# MAGIC
# MAGIC The pipeline simulates source CDC events, loads them into an append-only Bronze Delta table, applies schema-safe Delta merges into Silver current-state tables, validates warehouse quality, documents catalog metadata, and demonstrates Delta time travel restore.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC ```text
# MAGIC Source model
# MAGIC   -> Simulated CDC JSON events
# MAGIC   -> Databricks Volume landing path
# MAGIC   -> Auto Loader
# MAGIC   -> Bronze Delta table
# MAGIC   -> PySpark schema contracts
# MAGIC   -> Delta MERGE
# MAGIC   -> Silver current-state Delta tables
# MAGIC   -> Quality checks
# MAGIC   -> Catalog metadata
# MAGIC   -> Delta time travel restore
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Run Order
# MAGIC
# MAGIC 1. `00_setup_and_source_model`
# MAGIC 2. `01_generate_cdc_events`
# MAGIC 3. `02_bronze_ingest_delta`
# MAGIC 4. `03_apply_silver_merges`
# MAGIC 5. `06_cdc_correctness_tests`
# MAGIC 6. `05_quality_checks`
# MAGIC 7. `08_catalog_metadata`
# MAGIC 8. `04_restore_with_delta_time_travel`
# MAGIC 9. `09_final_summary_or_pr_notes`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Schema Design
# MAGIC
# MAGIC The source system models wallet and payment processing with five tables:
# MAGIC
# MAGIC - `customers`
# MAGIC - `wallet_accounts`
# MAGIC - `merchants`
# MAGIC - `payment_transactions`
# MAGIC - `payment_attempts`
# MAGIC
# MAGIC Strong entities are `customers`, `wallet_accounts`, `merchants`, and `payment_transactions`.
# MAGIC
# MAGIC `payment_attempts` is a weak entity because it depends on a parent payment transaction.
# MAGIC
# MAGIC The source design includes primary keys, foreign keys, indexes, timestamps, nullable optional fields, currency fields, status domains, non-negative balance checks, and positive payment amount checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Strategy
# MAGIC
# MAGIC CDC is simulated using JSON event files written to a Databricks Volume.
# MAGIC
# MAGIC Each CDC event contains:
# MAGIC
# MAGIC - `source_table`
# MAGIC - `cdc_operation`
# MAGIC - `cdc_sequence`
# MAGIC - `source_commit_ts`
# MAGIC - `schema_version`
# MAGIC - `payload_json`
# MAGIC
# MAGIC The sample CDC stream includes inserts, one update, and one delete.
# MAGIC
# MAGIC In production, this simulated source would be replaced by database logs, Debezium, Fivetran, Kafka, or another CDC connector. The downstream Bronze and Silver logic would use the same event contract.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lake And Warehouse Modeling
# MAGIC
# MAGIC The lake layer is:
# MAGIC
# MAGIC - `robustrade_dev.bronze.cdc_events`
# MAGIC
# MAGIC This table is append-only and keeps the full CDC history.
# MAGIC
# MAGIC The warehouse layer is:
# MAGIC
# MAGIC - `robustrade_dev.silver.customers_current`
# MAGIC - `robustrade_dev.silver.wallet_accounts_current`
# MAGIC - `robustrade_dev.silver.merchants_current`
# MAGIC - `robustrade_dev.silver.payment_transactions_current`
# MAGIC - `robustrade_dev.silver.payment_attempts_current`
# MAGIC
# MAGIC Silver tables use Delta `MERGE` to keep the latest non-deleted row per primary key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Change Safety
# MAGIC
# MAGIC Before writing to Silver, the pipeline validates each CDC payload against an explicit schema contract.
# MAGIC
# MAGIC The pipeline stops if:
# MAGIC
# MAGIC - a required column is missing
# MAGIC - a data type changes
# MAGIC - a required column becomes nullable
# MAGIC
# MAGIC This prevents incompatible source schema changes from silently corrupting downstream tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Parity
# MAGIC
# MAGIC Source business rules are mirrored in the warehouse quality notebook.
# MAGIC
# MAGIC Checks include:
# MAGIC
# MAGIC - primary key uniqueness
# MAGIC - referential integrity
# MAGIC - non-negative wallet balances
# MAGIC - positive payment amounts
# MAGIC - accepted transaction statuses
# MAGIC - duplicate CDC sequence detection
# MAGIC
# MAGIC The quality notebook raises an error if any check fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Recovery
# MAGIC
# MAGIC Recovery is handled in two ways:
# MAGIC
# MAGIC 1. Silver tables can be rebuilt from the append-only Bronze CDC log.
# MAGIC 2. Delta Lake time travel can restore tables using `DESCRIBE HISTORY` and `RESTORE TABLE`.
# MAGIC
# MAGIC The restore notebook demonstrates corrupting a development Silver table, restoring it to the previous version, and confirming the transaction status returns to `SETTLED`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Exposure
# MAGIC
# MAGIC Catalog metadata is documented in the `08_catalog_metadata` notebook.
# MAGIC
# MAGIC It includes:
# MAGIC
# MAGIC - dataset name
# MAGIC - layer
# MAGIC - platform
# MAGIC - owner
# MAGIC - description
# MAGIC - consumers
# MAGIC - primary key
# MAGIC - update cadence
# MAGIC
# MAGIC The notebook also adds Unity Catalog table comments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test And Validation Results
# MAGIC
# MAGIC The following validations were completed:
# MAGIC
# MAGIC - Part 2 generated 7 CDC events.
# MAGIC - Part 3 loaded 7 rows into `bronze.cdc_events`.
# MAGIC - Part 4 schema contracts passed and unsafe type change correctly failed.
# MAGIC - Part 5 Silver merges created current-state tables.
# MAGIC - `payment_transactions_current` shows transaction `t_001` as `SETTLED`.
# MAGIC - Part 6 CDC correctness checks passed.
# MAGIC - Rerunning the Silver merge did not create duplicate rows.
# MAGIC - Part 7 quality checks passed.
# MAGIC - Part 8 catalog metadata coverage passed.
# MAGIC - Part 9 Delta restore returned the corrupted transaction back to `SETTLED`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Status
# MAGIC
# MAGIC The project satisfies the CDC lakehouse reliability assignment requirements using Databricks, PySpark, and Delta Lake.
# MAGIC
# MAGIC It demonstrates source modeling, CDC ingestion, Bronze history retention, Silver current-state modeling, schema-safe stopping, validation parity, catalog exposure, CDC correctness testing, idempotent replay, and Delta time travel recovery.
