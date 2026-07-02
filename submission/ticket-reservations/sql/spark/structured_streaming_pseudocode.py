"""Spark Structured Streaming job for raw Kafka CDC files in S3.

The pipeline starts after Kafka events are landed as JSON files in S3:

raw S3 CDC files -> Spark every 5 minutes -> Iceberg silver
-> Snowflake refresh.

This is intentionally simple. The helper functions represent small SQL/model
steps that would live beside this job in production.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


RAW_CDC_PATH = "s3://ticketing-raw-cdc/events/"
CHECKPOINT_PATH = "s3://ticketing-lakehouse/checkpoints/raw-s3-cdc-to-iceberg-5min"

RAW_CDC_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("primary_key_json", StringType(), False),
        StructField("op", StringType(), False),
        StructField("before_json", StringType(), True),
        StructField("after_json", StringType(), True),
        StructField("schema_id", StringType(), False),
        StructField("source_lsn", LongType(), False),
        StructField("source_tx_id", StringType(), False),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", LongType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("source_event_at", TimestampType(), False),
        StructField("landed_at", TimestampType(), False),
    ]
)


def read_raw_s3_cdc_events() -> DataFrame:
    return (
        spark.readStream.format("json")
        .schema(RAW_CDC_SCHEMA)
        .option("maxFilesPerTrigger", 200)
        .option("latestFirst", "false")
        .load(RAW_CDC_PATH)
    )


def normalize_raw_events(raw_events: DataFrame) -> DataFrame:
    return raw_events.select(
        "event_id",
        F.lit("ticketing").alias("source_system"),
        F.lit("ticketing").alias("source_database"),
        F.lit("public").alias("source_schema"),
        "source_table",
        "primary_key_json",
        "op",
        "before_json",
        "after_json",
        "schema_id",
        "source_lsn",
        "source_tx_id",
        "kafka_topic",
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        "kafka_offset",
        "source_event_at",
        F.current_timestamp().alias("ingested_at"),
    )


def process_microbatch(raw_events: DataFrame, batch_id: int) -> None:
    silver_events = normalize_raw_events(raw_events)

    # Stop before any writes when an incompatible source change appears.
    assert_compatible_or_raise(silver_events)

    # Silver keeps every insert/update/delete event, idempotently by event_id.
    silver_events.createOrReplaceTempView("batch_cdc_events")
    spark.sql("""
        MERGE INTO silver.ticketing_cdc_events t
        USING batch_cdc_events s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Silver tables include the current state and history needed for restore.
    merge_silver_current_tables(silver_events)
    merge_silver_history_tables(silver_events)

    # Snowflake refresh reads the just-committed silver Iceberg snapshot.
    call_snowflake_refresh(batch_id=batch_id)


query = (
    read_raw_s3_cdc_events()
    .writeStream.foreachBatch(process_microbatch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 minutes")
    .queryName("ticketing_raw_s3_cdc_to_silver_5min")
    .start()
)
