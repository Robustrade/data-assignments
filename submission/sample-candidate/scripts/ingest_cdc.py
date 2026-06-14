import logging
from datetime import datetime

from pyspark.sql import SparkSession

from pipeline.cdc import CDCIngestion
from pipeline.bronze import BronzeLayer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


BRONZE_PATH = "data/bronze/cdc_events"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("CDC Ingestion")
        .getOrCreate()
    )


def fetch_cdc_events():
    """
    Assignment Scope

    Simulates WAL CDC events.

    Production:
        PostgreSQL WAL
            ->
        Debezium
            ->
        Kafka
            ->
        Spark
    """

    return [

        {
            "sequence_number": 1,

            "table_name": "customers",

            "operation": "INSERT",

            "primary_key": 1,

            "before_image": None,

            "after_image": {

                "customer_id": 1,

                "email": "john@example.com",

                "first_name": "John",

                "last_name": "Doe",

                "phone": "9999999999",

                "customer_status": "ACTIVE"
            },

            "event_timestamp":
                datetime.now()
        },

        {
            "sequence_number": 2,

            "table_name": "products",

            "operation": "INSERT",

            "primary_key": 100,

            "before_image": None,

            "after_image": {

                "product_id": 100,

                "sku": "SKU100",

                "product_name": "Laptop",

                "category": "Electronics",

                "unit_price": 50000,

                "product_status": "ACTIVE"
            },

            "event_timestamp":
                datetime.now()
        }
    ]


def main():

    spark = create_spark_session()

    try:

        logger.info(
            "=" * 50
        )

        logger.info(
            "Starting CDC ingestion"
        )

        events = fetch_cdc_events()

        logger.info(
            f"Fetched {len(events)} CDC events"
        )

        cdc_ingestion = CDCIngestion(
            spark
        )

        cdc_df = (
            cdc_ingestion
            .create_cdc_dataframe(
                events
            )
        )

        bronze_layer = BronzeLayer(
            spark=spark,
            bronze_path=BRONZE_PATH
        )

        bronze_layer.write_events(
            cdc_df
        )

        logger.info(
            "CDC events written to Bronze"
        )

        try:

            bronze_df = (
                bronze_layer.read_events()
            )

            max_sequence = (
                cdc_ingestion
                .latest_sequence(
                    bronze_df
                )
            )

            logger.info(
                f"Latest sequence: "
                f"{max_sequence}"
            )

        except Exception:

            logger.warning(
                "Unable to determine watermark"
            )

        logger.info(
            "CDC ingestion successful"
        )

        logger.info(
            "=" * 50
        )

    except Exception as e:

        logger.error(
            "CDC ingestion failed"
        )

        logger.exception(e)

        raise

    finally:

        spark.stop()


if __name__ == "__main__":

    main()