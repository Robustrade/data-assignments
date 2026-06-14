import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.warehouse import (
    CustomerDimensionBuilder,
    ProductDimensionBuilder,
    FactOrderBuilder,
    FactOrderItemBuilder,
    FactPaymentBuilder,
    WarehouseWriter
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


# =====================================================
# SILVER PATHS
# =====================================================

SILVER_CUSTOMERS_PATH = (
    "data/silver/customers"
)

SILVER_PRODUCTS_PATH = (
    "data/silver/products"
)

SILVER_ORDERS_PATH = (
    "data/silver/orders"
)

SILVER_ORDER_ITEMS_PATH = (
    "data/silver/order_items"
)

SILVER_PAYMENTS_PATH = (
    "data/silver/payment_attempts"
)


# =====================================================
# WAREHOUSE PATH
# =====================================================

WAREHOUSE_PATH = (
    "data/warehouse"
)


# =====================================================
# SPARK
# =====================================================

def create_spark_session():

    return (

        SparkSession

        .builder

        .appName(
            "Build Warehouse"
        )

        .getOrCreate()
    )


# =====================================================
# EMPTY DIMENSION HELPERS
# =====================================================

def create_empty_customer_dimension(
        spark
):

    return spark.createDataFrame(
        [],
        """
        customer_id BIGINT,
        email STRING,
        first_name STRING,
        last_name STRING,
        phone STRING,
        customer_status STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        """
    )


def create_empty_product_dimension(
        spark
):

    return spark.createDataFrame(
        [],
        """
        product_id BIGINT,
        sku STRING,
        product_name STRING,
        category STRING,
        unit_price DECIMAL(18,2),
        product_status STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
        """
    )


# =====================================================
# LOAD EXISTING DIMENSION
# =====================================================

def load_existing_dimension(
        spark,
        path,
        empty_df
):

    if Path(path).exists():

        return spark.read.parquet(path)

    return empty_df


# =====================================================
# MAIN
# =====================================================

def main():

    spark = create_spark_session()

    try:

        logger.info(
            "Starting warehouse build"
        )

        # ==========================================
        # READ SILVER TABLES
        # ==========================================

        customers_df = (
            spark.read.parquet(
                SILVER_CUSTOMERS_PATH
            )
        )

        products_df = (
            spark.read.parquet(
                SILVER_PRODUCTS_PATH
            )
        )

        orders_df = (
            spark.read.parquet(
                SILVER_ORDERS_PATH
            )
        )

        order_items_df = (
            spark.read.parquet(
                SILVER_ORDER_ITEMS_PATH
            )
        )

        payments_df = (
            spark.read.parquet(
                SILVER_PAYMENTS_PATH
            )
        )

        logger.info(
            "Silver tables loaded"
        )

        # ==========================================
        # LOAD EXISTING DIMENSIONS
        # ==========================================

        existing_customer_dim = (
            load_existing_dimension(
                spark,
                f"{WAREHOUSE_PATH}/dim_customer",
                create_empty_customer_dimension(
                    spark
                )
            )
        )

        existing_product_dim = (
            load_existing_dimension(
                spark,
                f"{WAREHOUSE_PATH}/dim_product",
                create_empty_product_dimension(
                    spark
                )
            )
        )

        # ==========================================
        # CUSTOMER DIMENSION
        # ==========================================

        customer_builder = (
            CustomerDimensionBuilder(
                spark
            )
        )

        dim_customer = (
            customer_builder.build(
                customers_df,
                existing_customer_dim
            )
        )

        logger.info(
            "Customer dimension built"
        )

        # ==========================================
        # PRODUCT DIMENSION
        # ==========================================

        product_builder = (
            ProductDimensionBuilder(
                spark
            )
        )

        dim_product = (
            product_builder.build(
                products_df,
                existing_product_dim
            )
        )

        logger.info(
            "Product dimension built"
        )

        # ==========================================
        # FACT TABLES
        # ==========================================

        fact_orders = (

            FactOrderBuilder

            .build(
                orders_df
            )
        )

        fact_order_items = (

            FactOrderItemBuilder

            .build(
                order_items_df
            )
        )

        fact_payments = (

            FactPaymentBuilder

            .build(
                payments_df
            )
        )

        logger.info(
            "Fact tables built"
        )

        # ==========================================
        # WRITE WAREHOUSE
        # ==========================================

        writer = WarehouseWriter(
            WAREHOUSE_PATH
        )

        writer.write_dimension(
            dim_customer,
            "dim_customer"
        )

        writer.write_dimension(
            dim_product,
            "dim_product"
        )

        writer.write_fact(
            fact_orders,
            "fact_orders"
        )

        writer.write_fact(
            fact_order_items,
            "fact_order_items"
        )

        writer.write_fact(
            fact_payments,
            "fact_payment_attempts"
        )

        logger.info(
            "Warehouse tables written"
        )

        logger.info(
            "Warehouse build successful"
        )

    except Exception as e:

        logger.exception(
            "Warehouse build failed"
        )

        raise

    finally:

        spark.stop()


if __name__ == "__main__":

    main()