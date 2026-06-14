# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

checks = [
    (
        "customers_current_pk_unique",
        f"""
        SELECT customer_id, COUNT(*) AS row_count
        FROM {catalog}.silver.customers_current
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """,
    ),
    (
        "wallets_have_customers",
        f"""
        SELECT w.wallet_id
        FROM {catalog}.silver.wallet_accounts_current w
        LEFT JOIN {catalog}.silver.customers_current c
          ON w.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    ),
    (
        "payments_have_wallets_and_customers",
        f"""
        SELECT p.transaction_id
        FROM {catalog}.silver.payment_transactions_current p
        LEFT JOIN {catalog}.silver.wallet_accounts_current w
          ON p.wallet_id = w.wallet_id
        LEFT JOIN {catalog}.silver.customers_current c
          ON p.customer_id = c.customer_id
        WHERE w.wallet_id IS NULL OR c.customer_id IS NULL
        """,
    ),
    (
        "attempts_have_transactions",
        f"""
        SELECT a.attempt_id
        FROM {catalog}.silver.payment_attempts_current a
        LEFT JOIN {catalog}.silver.payment_transactions_current p
          ON a.transaction_id = p.transaction_id
        WHERE p.transaction_id IS NULL
        """,
    ),
    (
        "non_negative_wallet_balances",
        f"""
        SELECT wallet_id
        FROM {catalog}.silver.wallet_accounts_current
        WHERE balance_minor < 0
        """,
    ),
    (
        "positive_payment_amounts",
        f"""
        SELECT transaction_id
        FROM {catalog}.silver.payment_transactions_current
        WHERE amount_minor <= 0
        """,
    ),
    (
        "valid_payment_statuses",
        f"""
        SELECT transaction_id
        FROM {catalog}.silver.payment_transactions_current
        WHERE transaction_status NOT IN ('AUTHORIZED', 'SETTLED', 'FAILED', 'REVERSED')
        """,
    ),
    (
        "bronze_has_no_duplicate_sequences",
        f"""
        SELECT source_table, cdc_sequence, COUNT(*) AS row_count
        FROM {catalog}.bronze.cdc_events
        GROUP BY source_table, cdc_sequence
        HAVING COUNT(*) > 1
        """,
    ),
]

# COMMAND ----------

failures = []

for check_name, sql in checks:
    failed_rows = spark.sql(sql)
    failure_count = failed_rows.count()

    if failure_count:
        print(f"FAILED: {check_name}")
        display(failed_rows)
        failures.append(f"{check_name}: {failure_count} failing rows")
    else:
        print(f"PASSED: {check_name}")

if failures:
    raise ValueError("Quality checks failed:\n" + "\n".join(failures))

print("All quality checks passed.")
