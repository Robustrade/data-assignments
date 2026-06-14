# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")
dbutils.widgets.text("volume_path", "/Volumes/robustrade_dev/audit/landing")

catalog = dbutils.widgets.get("catalog")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")

bronze_table = f"{catalog}.bronze.cdc_events"
source_path = f"{volume_path}/cdc_events"
checkpoint_path = f"{volume_path}/_checkpoints/bronze_cdc_events"
schema_location = f"{volume_path}/_schemas/bronze_cdc_events"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

# COMMAND ----------

from pyspark.sql import functions as F

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(source_path)
)

bronze = (
    raw.withColumn("source_commit_ts", F.to_timestamp("source_commit_ts"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("ingest_batch_id", F.expr("uuid()"))
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    bronze.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "false")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()

# COMMAND ----------

spark.sql(
    f"""
    ALTER TABLE {bronze_table} SET TBLPROPERTIES (
      'delta.appendOnly' = 'true',
      'quality' = 'bronze',
      'description' = 'Immutable CDC event log for wallet and payment sources'
    )
    """
)

# COMMAND ----------

display(spark.table(bronze_table).orderBy("cdc_sequence"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_row_count
# MAGIC FROM robustrade_dev.bronze.cdc_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL robustrade_dev.bronze.cdc_events;