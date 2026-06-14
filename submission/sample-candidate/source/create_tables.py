from pathlib import Path

from pyspark.sql import SparkSession


SOURCE_PATH = "data/source"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("Create Source Tables")
        .getOrCreate()
    )


def create_customers_table(spark):

    df = spark.createDataFrame(
        [],
        """
        customer_id BIGINT,
        email STRING,
        first_name STRING,
        last_name STRING,
        phone STRING,
        customer_status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
        """
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/customers"
    )


def create_products_table(spark):

    df = spark.createDataFrame(
        [],
        """
        product_id BIGINT,
        sku STRING,
        product_name STRING,
        category STRING,
        unit_price DECIMAL(18,2),
        product_status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
        """
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/products"
    )


def create_orders_table(spark):

    df = spark.createDataFrame(
        [],
        """
        order_id BIGINT,
        customer_id BIGINT,
        order_status STRING,
        total_amount DECIMAL(18,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
        """
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/orders"
    )


def create_order_items_table(spark):

    df = spark.createDataFrame(
        [],
        """
        order_item_id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity INTEGER,
        unit_price DECIMAL(18,2),
        line_amount DECIMAL(18,2),
        created_at TIMESTAMP
        """
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/order_items"
    )


def create_payment_attempts_table(spark):

    df = spark.createDataFrame(
        [],
        """
        payment_attempt_id BIGINT,
        order_id BIGINT,
        payment_status STRING,
        gateway_transaction_id STRING,
        amount DECIMAL(18,2),
        attempt_timestamp TIMESTAMP
        """
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/payment_attempts"
    )


def main():

    spark = create_spark_session()

    try:

        Path(SOURCE_PATH).mkdir(
            parents=True,
            exist_ok=True
        )

        create_customers_table(spark)

        create_products_table(spark)

        create_orders_table(spark)

        create_order_items_table(spark)

        create_payment_attempts_table(spark)

        print(
            "Source tables created successfully."
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    main()