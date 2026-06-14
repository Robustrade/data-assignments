# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")

catalog = dbutils.widgets.get("catalog")

bronze_table = f"{catalog}.bronze.cdc_events"

customers = f"{catalog}.silver.customers_current"
wallets = f"{catalog}.silver.wallet_accounts_current"
merchants = f"{catalog}.silver.merchants_current"
payments = f"{catalog}.silver.payment_transactions_current"
attempts = f"{catalog}.silver.payment_attempts_current"

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT cdc_operation, COUNT(*) AS event_count
        FROM {bronze_table}
        GROUP BY cdc_operation
        ORDER BY cdc_operation
        """
    )
)

# COMMAND ----------

bronze_count = spark.table(bronze_table).count()

assert bronze_count == 7, f"Expected 7 Bronze CDC events, got {bronze_count}"

print("Bronze event count check passed.")

# COMMAND ----------

payment_status = spark.sql(
    f"""
    SELECT transaction_status
    FROM {payments}
    WHERE transaction_id = 't_001'
    """
).collect()[0]["transaction_status"]

assert payment_status == "SETTLED", f"Expected SETTLED, got {payment_status}"

print("Payment update check passed.")

# COMMAND ----------

last_sequence = spark.sql(
    f"""
    SELECT last_cdc_sequence
    FROM {payments}
    WHERE transaction_id = 't_001'
    """
).collect()[0]["last_cdc_sequence"]

assert last_sequence == 5, f"Expected sequence 5, got {last_sequence}"

print("Latest sequence check passed.")

# COMMAND ----------

deleted_customer_count = spark.sql(
    f"""
    SELECT COUNT(*) AS row_count
    FROM {customers}
    WHERE customer_id = 'c_deleted'
    """
).collect()[0]["row_count"]

assert deleted_customer_count == 0, (
    f"Expected deleted customer to be absent from Silver, got {deleted_customer_count}"
)

print("Delete handling check passed.")

# COMMAND ----------

expected_counts = {
    "customers_current": 1,
    "wallet_accounts_current": 1,
    "merchants_current": 1,
    "payment_transactions_current": 1,
    "payment_attempts_current": 1,
}

for table_name, expected_count in expected_counts.items():
    actual_count = spark.table(f"{catalog}.silver.{table_name}").count()
    assert actual_count == expected_count, (
        f"{table_name}: expected {expected_count}, got {actual_count}"
    )

print("Silver row count checks passed.")

# COMMAND ----------

pk_checks = {
    "customers_current": "customer_id",
    "wallet_accounts_current": "wallet_id",
    "merchants_current": "merchant_id",
    "payment_transactions_current": "transaction_id",
    "payment_attempts_current": "attempt_id",
}

for table_name, pk in pk_checks.items():
    duplicate_count = spark.sql(
        f"""
        SELECT COUNT(*) AS duplicate_groups
        FROM (
            SELECT {pk}, COUNT(*) AS row_count
            FROM {catalog}.silver.{table_name}
            GROUP BY {pk}
            HAVING COUNT(*) > 1
        )
        """
    ).collect()[0]["duplicate_groups"]

    assert duplicate_count == 0, (
        f"{table_name}: expected no duplicate {pk}, got {duplicate_count}"
    )

print("Primary key duplicate checks passed.")

# COMMAND ----------

print("All CDC correctness checks passed.")