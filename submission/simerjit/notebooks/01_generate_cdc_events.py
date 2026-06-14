# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")
dbutils.widgets.text("volume_path", "/Volumes/robustrade_dev/audit/landing")

catalog = dbutils.widgets.get("catalog")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.audit")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.audit.landing")

# COMMAND ----------

import json

from pyspark.sql import Row
from pyspark.sql import functions as F

events = [
    Row(
        source_table="customers",
        cdc_operation="INSERT",
        cdc_sequence=1,
        source_commit_ts="2026-01-01T10:00:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "customer_id": "c_001",
                "email": "ada@example.com",
                "full_name": "Ada Lovelace",
                "phone_number": "+15550100",
                "kyc_level": "BASIC",
                "customer_status": "ACTIVE",
                "updated_at": "2026-01-01T10:00:00Z",
            }
        ),
    ),
    Row(
        source_table="wallet_accounts",
        cdc_operation="INSERT",
        cdc_sequence=2,
        source_commit_ts="2026-01-01T10:02:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "wallet_id": "w_001",
                "customer_id": "c_001",
                "currency": "USD",
                "balance_minor": 500000,
                "wallet_status": "OPEN",
                "updated_at": "2026-01-01T10:02:00Z",
            }
        ),
    ),
    Row(
        source_table="merchants",
        cdc_operation="INSERT",
        cdc_sequence=3,
        source_commit_ts="2026-01-01T10:03:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "merchant_id": "m_001",
                "merchant_name": "Analytical Engines Ltd",
                "merchant_category": "SOFTWARE",
                "merchant_status": "ACTIVE",
                "created_at": "2026-01-01T10:03:00Z",
                "updated_at": "2026-01-01T10:03:00Z",
            }
        ),
    ),
    Row(
        source_table="payment_transactions",
        cdc_operation="INSERT",
        cdc_sequence=4,
        source_commit_ts="2026-01-01T10:05:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "transaction_id": "t_001",
                "wallet_id": "w_001",
                "customer_id": "c_001",
                "merchant_id": "m_001",
                "amount_minor": 1999,
                "currency": "USD",
                "transaction_status": "AUTHORIZED",
                "failure_code": None,
                "event_time": "2026-01-01T10:04:59Z",
                "updated_at": "2026-01-01T10:05:00Z",
            }
        ),
    ),
    Row(
        source_table="payment_transactions",
        cdc_operation="UPDATE",
        cdc_sequence=5,
        source_commit_ts="2026-01-01T10:06:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "transaction_id": "t_001",
                "wallet_id": "w_001",
                "customer_id": "c_001",
                "merchant_id": "m_001",
                "amount_minor": 1999,
                "currency": "USD",
                "transaction_status": "SETTLED",
                "failure_code": None,
                "event_time": "2026-01-01T10:04:59Z",
                "updated_at": "2026-01-01T10:06:00Z",
            }
        ),
    ),
    Row(
        source_table="payment_attempts",
        cdc_operation="INSERT",
        cdc_sequence=6,
        source_commit_ts="2026-01-01T10:06:30Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "attempt_id": "a_001",
                "transaction_id": "t_001",
                "processor_name": "stripe",
                "attempt_status": "APPROVED",
                "failure_code": None,
                "attempted_at": "2026-01-01T10:05:01Z",
                "updated_at": "2026-01-01T10:06:30Z",
            }
        ),
    ),
    Row(
        source_table="customers",
        cdc_operation="DELETE",
        cdc_sequence=7,
        source_commit_ts="2026-01-01T10:08:00Z",
        schema_version=1,
        payload_json=json.dumps(
            {
                "customer_id": "c_deleted",
                "email": "removed@example.com",
                "full_name": "Removed Customer",
                "phone_number": None,
                "kyc_level": "BASIC",
                "customer_status": "CLOSED",
                "updated_at": "2026-01-01T10:08:00Z",
            }
        ),
    ),
]

df = spark.createDataFrame(events).withColumn("event_date", F.to_date("source_commit_ts"))

landing_path = f"{volume_path}/cdc_events"

(
    df.coalesce(1)
    .write.mode("overwrite")
    .partitionBy("event_date")
    .json(landing_path)
)

print(f"Wrote CDC JSON events to {landing_path}")

# COMMAND ----------

display(dbutils.fs.ls(f"{volume_path}/cdc_events"))

# COMMAND ----------

display(
    spark.read.json(f"{volume_path}/cdc_events")
    .orderBy("cdc_sequence")
)