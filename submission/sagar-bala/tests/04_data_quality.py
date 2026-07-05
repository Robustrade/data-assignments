# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality & Validation Audits

# COMMAND ----------
import os
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
dbutils.widgets.text("base_dir", "/tmp/delta_cdc_medallion")
base_dir = dbutils.widgets.get("base_dir")
bronze_base_path = os.path.join(base_dir, "bronze")
silver_base_path = os.path.join(base_dir, "silver")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Datasets

# COMMAND ----------
cust_df = spark.read.format("delta").load(os.path.join(silver_base_path, "customers"))
ord_df = spark.read.format("delta").load(os.path.join(silver_base_path, "orders"))
item_df = spark.read.format("delta").load(os.path.join(silver_base_path, "order_items"))
pay_df = spark.read.format("delta").load(os.path.join(silver_base_path, "payment_attempts"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## System Validations

# COMMAND ----------
# A. PK Uniqueness check
print("--- 1. PK Uniqueness ---")
pks = {
    "customers": ("customer_id", cust_df),
    "orders": ("order_id", ord_df),
    "order_items": ("order_item_id", item_df),
    "payment_attempts": ("payment_attempt_id", pay_df)
}

for name, (pk_col, df) in pks.items():
    total = df.count()
    distinct = df.select(pk_col).distinct().count()
    if total != distinct:
        print(f"[FAIL] PK violation on '{name}': {total - distinct} duplicates.")
    else:
        print(f"[PASS] PK check passed for '{name}'.")

# COMMAND ----------
# B. Referential Integrity (FK Checks)
print("\n--- 2. Referential Integrity ---")

# Verify orders -> customers
orphan_orders = ord_df.join(cust_df, "customer_id", "left_anti").count()
if orphan_orders > 0:
    print(f"[FAIL] Orphan orders found: {orphan_orders} rows mismatch customer_id.")
else:
    print("[PASS] FK check passed: orders -> customers.")
    
# Verify order_items -> orders
orphan_items = item_df.join(ord_df, "order_id", "left_anti").count()
if orphan_items > 0:
    print(f"[FAIL] Orphan items found: {orphan_items} rows mismatch order_id.")
else:
    print("[PASS] FK check passed: order_items -> orders.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Business Validations

# COMMAND ----------
# A. Aggregations (Order header amount matches sum of items)
print("\n--- 3. Header Aggregation totals ---")
item_sums = item_df.groupBy("order_id").agg(F.sum("item_subtotal").alias("calc_total"))
mismatches = ord_df.join(item_sums, "order_id", "inner") \
    .withColumn("diff", F.abs(F.col("total_amount") - F.col("calc_total"))) \
    .filter(F.col("diff") > 0.01)

mismatch_count = mismatches.count()
if mismatch_count > 0:
    print(f"[FAIL] Amount mismatches found on {mismatch_count} orders.")
    mismatches.select("order_id", "total_amount", "calc_total", "diff").show(5)
else:
    print("[PASS] Order headers match line item subtotals.")

# COMMAND ----------
# B. Timelines Chronology
print("\n--- 4. Timeline Chronology ---")
chronology_fails = ord_df.filter(
    F.col("shipped_at").isNotNull() & (F.col("shipped_at") < F.col("created_at"))
).count()

if chronology_fails > 0:
    print(f"[FAIL] Date chronology violation on {chronology_fails} orders.")
else:
    print("[PASS] Timeline chronology checks passed.")

# COMMAND ----------
# C. State Machine status transitions (using Bronze CDF history)
print("\n--- 5. State Machine Transition checks ---")
orders_bronze_path = os.path.join(bronze_base_path, "orders")

orders_cdf_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(orders_bronze_path)

pre_df = orders_cdf_df.filter(F.col("_change_type") == "update_preimage") \
    .select(F.col("order_id"), F.col("status").alias("old_status"), F.col("_commit_timestamp"))
    
post_df = orders_cdf_df.filter(F.col("_change_type") == "update_postimage") \
    .select(F.col("order_id"), F.col("status").alias("new_status"), F.col("_commit_timestamp"))

transitions = pre_df.join(post_df, ["order_id", "_commit_timestamp"], "inner")

# Flag any transitions reverting terminal statuses back to PENDING/PAID
violations = transitions.filter(
    ((F.col("old_status") == "SHIPPED") & F.col("new_status").isin("PENDING", "PAID")) |
    ((F.col("old_status") == "CANCELLED") & F.col("new_status").isin("PENDING", "PAID", "SHIPPED"))
)

violation_count = violations.count()
if violation_count > 0:
    print(f"[FAIL] State reversion found: {violation_count} illegal status updates.")
    violations.select("order_id", "old_status", "new_status", "_commit_timestamp").show(5)
else:
    print("[PASS] All order status updates are compliant.")
