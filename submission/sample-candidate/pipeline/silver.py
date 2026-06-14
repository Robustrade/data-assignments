import json

from pyspark.sql.functions import (
    col,
    udf
)

from pyspark.sql.types import (
    MapType,
    StringType
)


class SilverLayer:

    """
    Converts raw CDC records
    into curated datasets.
    """

    @staticmethod
    def parse_json_column():

        def parse_json(value):

            if value is None:
                return None

            return json.loads(value)

        return udf(
            parse_json,
            MapType(
                StringType(),
                StringType()
            )
        )

    def flatten_after_image(
            self,
            bronze_df
    ):

        parse_udf = (
            self.parse_json_column()
        )

        df = bronze_df.withColumn(
            "record",
            parse_udf(
                col("after_image")
            )
        )

        return df