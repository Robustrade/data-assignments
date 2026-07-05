# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregations (Customer Metrics)

# COMMAND ----------
import os
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
dbutils.widgets.text("base_dir", "/tmp/delta_cdc_medallion")
base_dir = dbutils.widgets.get("base_dir")
silver_base_path = os.path.join(base_dir, "silver")
gold_path = os.path.join(base_dir, "gold_customer_revenue_summary")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------
cust_df = spark.read.format("delta").load(os.path.join(silver_base_path, "customers"))
ord_df = spark.read.format("delta").load(os.path.join(silver_base_path, "orders"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compute Aggregations

# COMMAND ----------
# Calculate customer total sales and order count
gold_df = ord_df.join(cust_df, "customer_id", "inner") \
    .groupBy("customer_id", "email", "first_name", "last_name") \
    .agg(
        F.sum("total_amount").alias("total_purchased_amount"),
        F.count("order_id").alias("order_count"),
        F.current_timestamp().alias("computed_at")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Gold Table

# COMMAND ----------
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print(f"Gold: Updated customer metrics report at {gold_path}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Review Output

# COMMAND ----------
spark.read.format("delta").load(gold_path).show(10)
