# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Merge (Deduplicate & Merge)

# COMMAND ----------
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

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
# MAGIC ## Silver Schemas

# COMMAND ----------
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("email", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("shipping_address", StringType(), False),
    StructField("notes", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("shipped_at", TimestampType(), True)
])

order_items_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("item_subtotal", DoubleType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

payment_attempts_schema = StructType([
    StructField("payment_attempt_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("gateway", StringType(), False),
    StructField("status", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("attempted_at", TimestampType(), False)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Delta Merge Engine

# COMMAND ----------
def sync_bronze_to_silver(table_name: str, primary_key: str, base_schema: StructType):
    bronze_path = os.path.join(bronze_base_path, table_name)
    silver_path = os.path.join(silver_base_path, table_name)
    
    # Read Change Data Feed from Bronze
    cdf_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .load(bronze_path)
        
    # Deduplicate micro-batch by taking the latest mutation state per key
    window_spec = Window.partitionBy(primary_key).orderBy(
        F.col("_commit_timestamp").desc(),
        F.col("_processed_at").desc()
    )
    
    latest_changes_df = cdf_df \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")
        
    # Initialize Silver table if missing
    if not DeltaTable.isDeltaTable(spark, silver_path):
        fields = [field for field in base_schema.fields if field.name not in ["_change_type", "_commit_timestamp"]]
        fields.append(StructField("_last_updated_at", TimestampType(), True))
        silver_schema = StructType(fields)
        
        spark.createDataFrame(spark.sparkContext.emptyRDD(), silver_schema) \
            .write.format("delta").mode("overwrite").save(silver_path)
            
    # Apply MERGE
    silver_table = DeltaTable.forPath(spark, silver_path)
    
    non_update_cols = {primary_key, "_change_type", "_commit_timestamp", "_processed_at"}
    update_set = {col: f"updates.{col}" for col in latest_changes_df.columns if col not in non_update_cols}
    update_set["_last_updated_at"] = "current_timestamp()"
    
    insert_set = {col: f"updates.{col}" for col in latest_changes_df.columns if col not in ["_change_type", "_commit_timestamp", "_processed_at"]}
    insert_set["_last_updated_at"] = "current_timestamp()"
    
    print(f"Silver: Merging CDC into '{table_name}'...")
    silver_table.alias("target") \
        .merge(
            latest_changes_df.alias("updates"),
            f"target.{primary_key} = updates.{primary_key}"
        ) \
        .whenMatchedDelete(
            condition="updates._change_type = 'delete' OR updates._change_type = 'DELETE'"
        ) \
        .whenMatchedUpdate(
            condition="updates._change_type != 'delete' AND updates._change_type != 'DELETE'",
            set=update_set
        ) \
        .whenNotMatchedInsert(
            condition="updates._change_type != 'delete' AND updates._change_type != 'DELETE'",
            values=insert_set
        ) \
        .execute()
    print(f"Silver: Sync completed for '{table_name}'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sync Datasets

# COMMAND ----------
sync_bronze_to_silver("customers", "customer_id", customers_schema)
sync_bronze_to_silver("orders", "order_id", orders_schema)
sync_bronze_to_silver("order_items", "order_item_id", order_items_schema)
sync_bronze_to_silver("payment_attempts", "payment_attempt_id", payment_attempts_schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Restore & Time Travel Demo

# COMMAND ----------
def demo_time_travel_and_restore(table_name: str, primary_key: str):
    path = os.path.join(silver_base_path, table_name)
    delta_table = DeltaTable.forPath(spark, path)
    
    print(f"\n--- History log for '{table_name}' ---")
    delta_table.history().select("version", "timestamp", "operation").show(truncate=False)
    
    # Query Version 0
    print(f"Querying '{table_name}' at version 0...")
    spark.read.format("delta").option("versionAsOf", 0).load(path).show(5)
    
    # Rollback table to original state
    print(f"Restoring table '{table_name}' back to version 0...")
    delta_table.restoreToVersion(0)
    
    print(f"Current state of '{table_name}' after rollback:")
    spark.read.format("delta").load(path).show(5)

# COMMAND ----------
demo_time_travel_and_restore("orders", "order_id")
