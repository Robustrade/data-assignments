# Databricks notebook source
# MAGIC %md
# MAGIC # Catalog Metadata
# MAGIC
# MAGIC This notebook documents the lake and warehouse datasets created by the CDC lakehouse pipeline.
# MAGIC
# MAGIC The assignment asks for catalog exposure. In production, this metadata could be published to Unity Catalog, OpenMetadata, DataHub, or another governance catalog.
# MAGIC
# MAGIC For this assignment, I maintain catalog metadata inside this notebook. It documents:
# MAGIC
# MAGIC - dataset name
# MAGIC - layer
# MAGIC - platform
# MAGIC - owner
# MAGIC - description
# MAGIC - consumers
# MAGIC - primary key
# MAGIC - update cadence

# COMMAND ----------

catalog_metadata = {
    "version": 1,
    "owner": "data-platform",
    "domain": "wallet-payments",
    "datasets": [
        {
            "name": "robustrade_dev.bronze.cdc_events",
            "layer": "lake",
            "platform": "Databricks Delta Lake",
            "description": "Append-only CDC event log retaining every source insert, update, and delete.",
            "consumers": ["data engineering", "incident response", "backfill jobs"],
            "update_cadence": "near real time via Auto Loader",
            "metadata": {
                "primary_ordering": "source_commit_ts, cdc_sequence",
                "retention": "full assignment history",
            },
        },
        {
            "name": "robustrade_dev.silver.customers_current",
            "layer": "warehouse",
            "platform": "Databricks Delta Lake",
            "description": "Latest non-deleted customer state.",
            "primary_key": "customer_id",
            "consumers": ["analytics", "risk", "customer operations"],
            "update_cadence": "after each CDC merge job",
        },
        {
            "name": "robustrade_dev.silver.wallet_accounts_current",
            "layer": "warehouse",
            "platform": "Databricks Delta Lake",
            "description": "Latest wallet balance and status.",
            "primary_key": "wallet_id",
            "consumers": ["analytics", "finance operations"],
            "update_cadence": "after each CDC merge job",
        },
        {
            "name": "robustrade_dev.silver.merchants_current",
            "layer": "warehouse",
            "platform": "Databricks Delta Lake",
            "description": "Latest merchant reference state.",
            "primary_key": "merchant_id",
            "consumers": ["analytics", "risk"],
            "update_cadence": "after each CDC merge job",
        },
        {
            "name": "robustrade_dev.silver.payment_transactions_current",
            "layer": "warehouse",
            "platform": "Databricks Delta Lake",
            "description": "Latest payment transaction state.",
            "primary_key": "transaction_id",
            "consumers": ["analytics", "finance", "reconciliation"],
            "update_cadence": "after each CDC merge job",
        },
        {
            "name": "robustrade_dev.silver.payment_attempts_current",
            "layer": "warehouse",
            "platform": "Databricks Delta Lake",
            "description": "Weak entity containing processor attempts for each payment transaction.",
            "primary_key": "attempt_id",
            "consumers": ["payments operations", "incident response"],
            "update_cadence": "after each CDC merge job",
        },
    ],
}

# COMMAND ----------

display(spark.createDataFrame(catalog_metadata["datasets"]))

# COMMAND ----------

expected_datasets = {
    "robustrade_dev.bronze.cdc_events",
    "robustrade_dev.silver.customers_current",
    "robustrade_dev.silver.wallet_accounts_current",
    "robustrade_dev.silver.merchants_current",
    "robustrade_dev.silver.payment_transactions_current",
    "robustrade_dev.silver.payment_attempts_current",
}

actual_datasets = {dataset["name"] for dataset in catalog_metadata["datasets"]}

missing = expected_datasets - actual_datasets

assert not missing, f"Missing catalog entries: {missing}"

print("Catalog metadata coverage check passed.")

# COMMAND ----------

table_comments = {
    "bronze.cdc_events": "Append-only CDC event log retaining every source insert, update, and delete.",
    "silver.customers_current": "Latest non-deleted customer state.",
    "silver.wallet_accounts_current": "Latest wallet balance and status.",
    "silver.merchants_current": "Latest merchant reference state.",
    "silver.payment_transactions_current": "Latest payment transaction state.",
    "silver.payment_attempts_current": "Weak entity containing processor attempts for each payment transaction.",
}

for table_name, comment in table_comments.items():
    spark.sql(f"COMMENT ON TABLE robustrade_dev.{table_name} IS '{comment}'")

print("Unity Catalog table comments updated.")