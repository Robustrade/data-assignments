import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row


class CDCIngestion:

    """
    Production:

    PostgreSQL WAL
        ->
    Debezium
        ->
    Kafka
        ->
    Spark

    Assignment:

    CDC events are provided
    as JSON records.
    """

    def __init__(
            self,
            spark: SparkSession
    ):

        self.spark = spark

    def create_cdc_dataframe(
            self,
            events: list
    ):

        rows = []

        for event in events:

            rows.append(

                Row(
                    sequence_number=event["sequence_number"],
                    table_name=event["table_name"],
                    operation=event["operation"],
                    primary_key=str(
                        event["primary_key"]
                    ),
                    before_image=json.dumps(
                        event.get("before_image")
                    )
                    if event.get("before_image")
                    else None,
                    after_image=json.dumps(
                        event.get("after_image")
                    )
                    if event.get("after_image")
                    else None,
                    event_timestamp=event[
                        "event_timestamp"
                    ]
                )
            )

        return self.spark.createDataFrame(
            rows
        )

    def latest_sequence(
            self,
            bronze_df
    ):

        result = bronze_df.agg(
            {
                "sequence_number": "max"
            }
        ).collect()[0][0]

        return result if result else 0
    
    CDC_SCHEMA = StructType([
    StructField("sequence_number", LongType(), False),
    StructField("table_name", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("primary_key", StringType(), False),
    StructField("before_image", StringType(), True),
    StructField("after_image", StringType(), True),
    StructField("event_timestamp", TimestampType(), False)
    ])