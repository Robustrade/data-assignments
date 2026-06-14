# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")
dbutils.widgets.text("table_name", "payment_transactions_current")
dbutils.widgets.text("restore_version", "")

catalog = dbutils.widgets.get("catalog")
table_name = dbutils.widgets.get("table_name")
restore_version = dbutils.widgets.get("restore_version")

full_table = f"{catalog}.silver.{table_name}"

# COMMAND ----------

display(spark.table(full_table))

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {full_table}")
display(history)

# COMMAND ----------

spark.sql(
    f"""
    UPDATE {full_table}
    SET transaction_status = 'CORRUPTED_FOR_RESTORE_DEMO'
    WHERE transaction_id = 't_001'
    """
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT transaction_id, transaction_status
        FROM {full_table}
        WHERE transaction_id = 't_001'
        """
    )
)

# COMMAND ----------

history_after_bad_update = spark.sql(f"DESCRIBE HISTORY {full_table}")
display(history_after_bad_update)

# COMMAND ----------

history_rows = spark.sql(f"DESCRIBE HISTORY {full_table}").select("version").orderBy("version").collect()

versions = [row["version"] for row in history_rows]

restore_to_version = versions[-2]

spark.sql(f"RESTORE TABLE {full_table} TO VERSION AS OF {restore_to_version}")

print(f"Restored {full_table} to version {restore_to_version}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT transaction_id, transaction_status
        FROM {full_table}
        WHERE transaction_id = 't_001'
        """
    )
)

# COMMAND ----------

if restore_version:
    spark.sql(f"RESTORE TABLE {full_table} TO VERSION AS OF {int(restore_version)}")
    print(f"Restored {full_table} to Delta version {restore_version}")
else:
    print("No restore_version widget supplied. Demo restore already completed above.")