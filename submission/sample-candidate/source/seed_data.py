from decimal import Decimal
from datetime import datetime

from pyspark.sql import SparkSession


SOURCE_PATH = "data/source"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("Seed Source Data")
        .getOrCreate()
    )


def seed_customers(spark):

    customers = [

        (
            1,
            "john@example.com",
            "John",
            "Doe",
            "9999999999",
            "ACTIVE",
            datetime.now(),
            datetime.now()
        ),

        (
            2,
            "alice@example.com",
            "Alice",
            "Smith",
            None,
            "ACTIVE",
            datetime.now(),
            datetime.now()
        )
    ]

    df = spark.createDataFrame(
        customers,
        [
            "customer_id",
            "email",
            "first_name",
            "last_name",
            "phone",
            "customer_status",
            "created_at",
            "updated_at"
        ]
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/customers"
    )


def seed_products(spark):

    products = [

        (
            100,
            "SKU100",
            "Laptop",
            "Electronics",
            Decimal("50000.00"),
            "ACTIVE",
            datetime.now(),
            datetime.now()
        ),

        (
            101,
            "SKU101",
            "Mouse",
            "Accessories",
            Decimal("1500.00"),
            "ACTIVE",
            datetime.now(),
            datetime.now()
        )
    ]

    df = spark.createDataFrame(
        products,
        [
            "product_id",
            "sku",
            "product_name",
            "category",
            "unit_price",
            "product_status",
            "created_at",
            "updated_at"
        ]
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/products"
    )


def seed_orders(spark):

    orders = [

        (
            1000,
            1,
            "PAID",
            Decimal("51500.00"),
            datetime.now(),
            datetime.now()
        )
    ]

    df = spark.createDataFrame(
        orders,
        [
            "order_id",
            "customer_id",
            "order_status",
            "total_amount",
            "created_at",
            "updated_at"
        ]
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/orders"
    )


def seed_order_items(spark):

    items = [

        (
            1,
            1000,
            100,
            1,
            Decimal("50000.00"),
            Decimal("50000.00"),
            datetime.now()
        ),

        (
            2,
            1000,
            101,
            1,
            Decimal("1500.00"),
            Decimal("1500.00"),
            datetime.now()
        )
    ]

    df = spark.createDataFrame(
        items,
        [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_amount",
            "created_at"
        ]
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/order_items"
    )


def seed_payment_attempts(spark):

    payments = [

        (
            1,
            1000,
            "SUCCESS",
            "TXN123456",
            Decimal("51500.00"),
            datetime.now()
        )
    ]

    df = spark.createDataFrame(
        payments,
        [
            "payment_attempt_id",
            "order_id",
            "payment_status",
            "gateway_transaction_id",
            "amount",
            "attempt_timestamp"
        ]
    )

    df.write.mode("overwrite").parquet(
        f"{SOURCE_PATH}/payment_attempts"
    )


def main():

    spark = create_spark_session()

    try:

        seed_customers(spark)

        seed_products(spark)

        seed_orders(spark)

        seed_order_items(spark)

        seed_payment_attempts(spark)

        print(
            "Source data seeded successfully."
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    main()