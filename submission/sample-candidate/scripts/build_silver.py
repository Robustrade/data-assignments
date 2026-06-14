import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


BRONZE_PATH = "data/bronze/cdc_events"

SILVER_CUSTOMERS_PATH = "data/silver/customers"
SILVER_PRODUCTS_PATH = "data/silver/products"
SILVER_ORDERS_PATH = "data/silver/orders"
SILVER_ORDER_ITEMS_PATH = "data/silver/order_items"
SILVER_PAYMENTS_PATH = "data/silver/payment_attempts"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("Build Silver Layer")
        .getOrCreate()
    )


def extract_json_field(column_name, field_name):

    return F.get_json_object(
        F.col(column_name),
        f"$.{field_name}"
    )


def build_latest_snapshot(
        bronze_df,
        table_name
):

    table_df = (

        bronze_df

        .filter(
            F.col("table_name") == table_name
        )

        .filter(
            F.col("operation") != "DELETE"
        )
    )

    window_spec = (

        Window

        .partitionBy("primary_key")

        .orderBy(
            F.col(
                "sequence_number"
            ).desc()
        )
    )

    latest_df = (

        table_df

        .withColumn(
            "rn",
            F.row_number().over(
                window_spec
            )
        )

        .filter(
            F.col("rn") == 1
        )

        .drop("rn")
    )

    return latest_df


def build_customers(bronze_df):

    latest_df = build_latest_snapshot(
        bronze_df,
        "customers"
    )

    return (

        latest_df

        .select(

            extract_json_field(
                "after_image",
                "customer_id"
            ).cast("bigint").alias(
                "customer_id"
            ),

            extract_json_field(
                "after_image",
                "email"
            ).alias(
                "email"
            ),

            extract_json_field(
                "after_image",
                "first_name"
            ).alias(
                "first_name"
            ),

            extract_json_field(
                "after_image",
                "last_name"
            ).alias(
                "last_name"
            ),

            extract_json_field(
                "after_image",
                "phone"
            ).alias(
                "phone"
            ),

            extract_json_field(
                "after_image",
                "customer_status"
            ).alias(
                "customer_status"
            )
        )
    )


def build_products(bronze_df):

    latest_df = build_latest_snapshot(
        bronze_df,
        "products"
    )

    return (

        latest_df

        .select(

            extract_json_field(
                "after_image",
                "product_id"
            ).cast("bigint").alias(
                "product_id"
            ),

            extract_json_field(
                "after_image",
                "sku"
            ).alias(
                "sku"
            ),

            extract_json_field(
                "after_image",
                "product_name"
            ).alias(
                "product_name"
            ),

            extract_json_field(
                "after_image",
                "category"
            ).alias(
                "category"
            ),

            extract_json_field(
                "after_image",
                "unit_price"
            ).cast(
                "decimal(18,2)"
            ).alias(
                "unit_price"
            ),

            extract_json_field(
                "after_image",
                "product_status"
            ).alias(
                "product_status"
            )
        )
    )


def build_orders(bronze_df):

    latest_df = build_latest_snapshot(
        bronze_df,
        "orders"
    )

    return (

        latest_df

        .select(

            extract_json_field(
                "after_image",
                "order_id"
            ).cast("bigint").alias(
                "order_id"
            ),

            extract_json_field(
                "after_image",
                "customer_id"
            ).cast("bigint").alias(
                "customer_id"
            ),

            extract_json_field(
                "after_image",
                "order_status"
            ).alias(
                "order_status"
            ),

            extract_json_field(
                "after_image",
                "total_amount"
            ).cast(
                "decimal(18,2)"
            ).alias(
                "total_amount"
            )
        )
    )


def build_order_items(bronze_df):

    latest_df = build_latest_snapshot(
        bronze_df,
        "order_items"
    )

    return (

        latest_df

        .select(

            extract_json_field(
                "after_image",
                "order_item_id"
            ).cast("bigint").alias(
                "order_item_id"
            ),

            extract_json_field(
                "after_image",
                "order_id"
            ).cast("bigint").alias(
                "order_id"
            ),

            extract_json_field(
                "after_image",
                "product_id"
            ).cast("bigint").alias(
                "product_id"
            ),

            extract_json_field(
                "after_image",
                "quantity"
            ).cast("int").alias(
                "quantity"
            ),

            extract_json_field(
                "after_image",
                "unit_price"
            ).cast(
                "decimal(18,2)"
            ).alias(
                "unit_price"
            ),

            extract_json_field(
                "after_image",
                "line_amount"
            ).cast(
                "decimal(18,2)"
            ).alias(
                "line_amount"
            )
        )
    )


def build_payment_attempts(bronze_df):

    latest_df = build_latest_snapshot(
        bronze_df,
        "payment_attempts"
    )

    return (

        latest_df

        .select(

            extract_json_field(
                "after_image",
                "payment_attempt_id"
            ).cast("bigint").alias(
                "payment_attempt_id"
            ),

            extract_json_field(
                "after_image",
                "order_id"
            ).cast("bigint").alias(
                "order_id"
            ),

            extract_json_field(
                "after_image",
                "payment_status"
            ).alias(
                "payment_status"
            ),

            extract_json_field(
                "after_image",
                "gateway_transaction_id"
            ).alias(
                "gateway_transaction_id"
            ),

            extract_json_field(
                "after_image",
                "amount"
            ).cast(
                "decimal(18,2)"
            ).alias(
                "amount"
            )
        )
    )


def main():

    spark = create_spark_session()

    try:

        logger.info(
            "Reading Bronze CDC events..."
        )

        bronze_df = (

            spark.read.parquet(
                BRONZE_PATH
            )
        )

        customers_df = build_customers(
            bronze_df
        )

        products_df = build_products(
            bronze_df
        )

        orders_df = build_orders(
            bronze_df
        )

        order_items_df = build_order_items(
            bronze_df
        )

        payments_df = build_payment_attempts(
            bronze_df
        )

        customers_df.write.mode(
            "overwrite"
        ).parquet(
            SILVER_CUSTOMERS_PATH
        )

        products_df.write.mode(
            "overwrite"
        ).parquet(
            SILVER_PRODUCTS_PATH
        )

        orders_df.write.mode(
            "overwrite"
        ).parquet(
            SILVER_ORDERS_PATH
        )

        order_items_df.write.mode(
            "overwrite"
        ).parquet(
            SILVER_ORDER_ITEMS_PATH
        )

        payments_df.write.mode(
            "overwrite"
        ).parquet(
            SILVER_PAYMENTS_PATH
        )

        logger.info(
            "Silver layer built successfully"
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    main()