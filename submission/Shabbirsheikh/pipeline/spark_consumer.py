"""
Processing layer: Kafka -> Spark Structured Streaming -> Parquet lake +
DuckDB warehouse.

Lake: events appended to Parquet via Spark's native streaming sink,
partitioned by day (from captured_at), so a single day can be targeted
directly later without scanning the whole lake.

Warehouse: DuckDB is single-writer, so Spark executors can't write to it
directly. Each microbatch is instead collected to the driver and handed to
the same apply_lake_events() the test suite already covers - Spark's job
here is just "get Kafka data to the driver, in order".

Restart-safety comes from Spark's own checkpoint (CHECKPOINT_PATH); the
_cdc_seq guard in apply_lake_events is a second, independent safety net.

Two run modes: on-demand (default, trigger(availableNow=True), processes
what's in Kafka and exits) or continuous (--continuous, keeps polling and
leaves a Spark driver process resident - stop it when done).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))  # spark-submit only puts the script's own dir on sys.path

import duckdb
from pyspark.sql import SparkSession, functions as F, types as T

from pipeline import warehouse as wh

KAFKA_BOOTSTRAP = "192.168.1.11:9092"
TOPIC = "cdc-events-v4"

LAKE_PATH = str(PROJECT_ROOT / "data" / "lake_parquet")
WAREHOUSE_DB = str(PROJECT_ROOT / "data" / "warehouse.duckdb")
CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "spark_checkpoints" / "main")

# `data` stays a raw JSON string - apply_lake_events() parses it per-table
ENVELOPE_SCHEMA = T.StructType([
    T.StructField("operation", T.StringType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("primary_key", T.StringType()),
    T.StructField("data", T.StringType()),
    T.StructField("captured_at", T.StringType()),
])


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("cdc-lakehouse").getOrCreate()


def run(
    spark: SparkSession | None = None,
    continuous: bool = False,
    trigger_interval: str = "5 seconds",
):
    owns_spark = spark is None
    spark = spark or build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = raw.select(
        F.col("offset").alias("sequence"),
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("event"),
    ).select(
        "sequence",
        F.col("event.operation").alias("operation"),
        F.col("event.table_name").alias("table_name"),
        F.col("event.primary_key").alias("primary_key"),
        F.col("event.data").alias("data"),
        F.col("event.captured_at").alias("captured_at"),
        F.to_date(F.to_timestamp(F.col("event.captured_at"))).alias("event_date"),  # lake partition key
    )

    bootstrap_conn = duckdb.connect(WAREHOUSE_DB)
    wh.create_warehouse_tables(bootstrap_conn)
    bootstrap_conn.close()

    def process_batch(batch_df, batch_id: int):
        batch_df.persist()
        try:
            if batch_df.take(1):
                (
                    batch_df.write
                    .format("parquet")
                    .mode("append")
                    .partitionBy("event_date")
                    .save(LAKE_PATH)
                )

                rows = [
                    r.asDict()
                    for r in batch_df.drop("event_date").orderBy("sequence").collect()
                ]

                # fresh connection per batch, not one held for the job's
                # lifetime - DuckDB locks the file for as long as a
                # connection is open, and continuous mode can run for hours
                wh_conn = duckdb.connect(WAREHOUSE_DB)
                try:
                    wh.apply_lake_events(wh_conn, rows)
                finally:
                    wh_conn.close()
                print(f"[batch {batch_id}] applied {len(rows)} event(s)", flush=True)
        finally:
            batch_df.unpersist()

    writer = (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
    )

    if continuous:
        print(f"Starting CONTINUOUS mode, checking every {trigger_interval} - Ctrl+C to stop", flush=True)
        query = writer.trigger(processingTime=trigger_interval).start()
    else:
        query = writer.trigger(availableNow=True).start()

    query.awaitTermination()

    if owns_spark:
        spark.stop()


if __name__ == "__main__":
    run(continuous="--continuous" in sys.argv)
