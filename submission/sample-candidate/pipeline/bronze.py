from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType
)


class BronzeLayer:

    def __init__(
            self,
            spark: SparkSession,
            bronze_path: str
    ):

        self.spark = spark

        self.bronze_path = bronze_path

    @staticmethod
    def bronze_schema():

        return StructType([

            StructField(
                "sequence_number",
                LongType(),
                False
            ),

            StructField(
                "table_name",
                StringType(),
                False
            ),

            StructField(
                "operation",
                StringType(),
                False
            ),

            StructField(
                "primary_key",
                StringType(),
                False
            ),

            StructField(
                "before_image",
                StringType(),
                True
            ),

            StructField(
                "after_image",
                StringType(),
                True
            ),

            StructField(
                "event_timestamp",
                TimestampType(),
                False
            )
        ])

    def write_events(
            self,
            events_df
    ):

        (
            events_df
            .write
            .mode("append")
            .parquet(self.bronze_path)
        )

    def read_events(self):

        return (
            self.spark
            .read
            .parquet(self.bronze_path)
        )