# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion

# COMMAND ----------
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
dbutils.widgets.text("base_dir", "/tmp/delta_cdc_medallion")
base_dir = dbutils.widgets.get("base_dir")
bronze_base_path = os.path.join(base_dir, "bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Source Schemas

# COMMAND ----------
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("email", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("_change_type", StringType(), False),
    StructField("_commit_timestamp", TimestampType(), False)
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
    StructField("shipped_at", TimestampType(), True),
    StructField("_change_type", StringType(), False),
    StructField("_commit_timestamp", TimestampType(), False)
])

order_items_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("item_subtotal", DoubleType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("_change_type", StringType(), False),
    StructField("_commit_timestamp", TimestampType(), False)
])

payment_attempts_schema = StructType([
    StructField("payment_attempt_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("gateway", StringType(), False),
    StructField("status", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("attempted_at", TimestampType(), False),
    StructField("_change_type", StringType(), False),
    StructField("_commit_timestamp", TimestampType(), False)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema Compatibility Checks

# COMMAND ----------
def check_schema_compatibility(table_name: str, incoming_fields: dict, target_fields: dict):
    # Check for dropped columns
    for col_name in target_fields:
        if col_name not in ["_processed_at", "_last_updated_at"] and col_name not in incoming_fields:
            raise ValueError(
                f"STOP THE LINE: Column '{col_name}' was dropped from source table '{table_name}'."
            )
            
    # Check for type mismatches
    for col_name, field in incoming_fields.items():
        if col_name in target_fields:
            target_type = target_fields[col_name].dataType.simpleString()
            incoming_type = field.dataType.simpleString()
            if target_type != incoming_type:
                raise TypeError(
                    f"STOP THE LINE: Column '{col_name}' type mismatch in '{table_name}'. "
                    f"Source: {incoming_type}, Target: {target_type}."
                )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingestion Writer

# COMMAND ----------
def write_to_bronze(table_name: str, incoming_df):
    path = os.path.join(bronze_base_path, table_name)
    
    if DeltaTable.isDeltaTable(spark, path):
        target_schema = spark.read.format("delta").load(path).schema
        target_fields = {field.name: field for field in target_schema.fields}
        incoming_fields = {field.name: field for field in incoming_df.schema.fields}
        check_schema_compatibility(table_name, incoming_fields, target_fields)
        
    bronze_df = incoming_df.withColumn("_processed_at", F.current_timestamp())
    
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .save(path)
        
    # Enable CDF on Bronze table for downstream incremental merges
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"Bronze: Appended and set CDF for {table_name}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingest Simulation Data (Batch 1)

# COMMAND ----------
cust_data = [
    ("cust_001", "alice@example.com", "Alice", "Smith", "ACTIVE", datetime(2026,7,6,0,0,0), datetime(2026,7,6,0,0,0), "insert", datetime(2026,7,6,0,0,0)),
    ("cust_002", "bob@example.com", "Bob", "Jones", "ACTIVE", datetime(2026,7,6,0,5,0), datetime(2026,7,6,0,5,0), "insert", datetime(2026,7,6,0,5,0))
]
ord_data = [
    ("ord_101", "cust_001", "PENDING", 150.50, "USD", "123 Main St", "Leave at door", datetime(2026,7,6,0,15,0), datetime(2026,7,6,0,15,0), None, "insert", datetime(2026,7,6,0,15,0)),
    ("ord_102", "cust_002", "PENDING", 45.00, "USD", "456 Elm St", None, datetime(2026,7,6,0,20,0), datetime(2026,7,6,0,20,0), None, "insert", datetime(2026,7,6,0,20,0))
]
items_data = [
    ("item_201", "ord_101", "prod_abc", 1, 100.00, 100.00, datetime(2026,7,6,0,15,0), datetime(2026,7,6,0,15,0), "insert", datetime(2026,7,6,0,15,0)),
    ("item_202", "ord_101", "prod_xyz", 2, 25.25, 50.50, datetime(2026,7,6,0,15,0), datetime(2026,7,6,0,15,0), "insert", datetime(2026,7,6,0,15,0)),
    ("item_203", "ord_102", "prod_def", 3, 15.00, 45.00, datetime(2026,7,6,0,20,0), datetime(2026,7,6,0,20,0), "insert", datetime(2026,7,6,0,20,0))
]
pay_data = [
    ("pay_301", "ord_101", 150.50, "STRIPE", "PENDING", None, datetime(2026,7,6,0,16,0), "insert", datetime(2026,7,6,0,16,0))
]

df_cust = spark.createDataFrame(cust_data, schema=customers_schema)
df_ord = spark.createDataFrame(ord_data, schema=orders_schema)
df_items = spark.createDataFrame(items_data, schema=order_items_schema)
df_pay = spark.createDataFrame(pay_data, schema=payment_attempts_schema)

write_to_bronze("customers", df_cust)
write_to_bronze("orders", df_ord)
write_to_bronze("order_items", df_items)
write_to_bronze("payment_attempts", df_pay)
