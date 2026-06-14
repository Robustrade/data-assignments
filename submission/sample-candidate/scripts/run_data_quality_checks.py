from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityException(Exception):
    pass


class DataQualityChecks:

    def create_spark_session():
        return (
            SparkSession.builder
            .appName("Run Data Quality Checks")
            .getOrCreate()
        )

    @staticmethod
    def validate_pk_uniqueness(
            df: DataFrame,
            pk_column: str
    ):

        duplicates = (

            df.groupBy(pk_column)

            .count()

            .filter(
                F.col("count") > 1
            )
        )

        if duplicates.count() > 0:

            raise DataQualityException(
                f"Duplicate PK found in {pk_column}"
            )

    @staticmethod
    def validate_not_null(
            df: DataFrame
            # column_name: str
    ):

        for column_name in df.columns():
            failures = (

                df.filter(
                    F.col(column_name).isNull()
                )
            )

            if failures.count() > 0:

                raise DataQualityException(
                    f"Null values found in {df}.{column_name}"
                )

    @staticmethod
    def validate_fk(
            child_df: DataFrame,
            parent_df: DataFrame,
            fk_column: str
    ):

        invalid_records = (

            child_df.alias("child")

            .join(
                parent_df.alias("parent"),
                fk_column,
                "leftanti"
            )
        )

        if invalid_records.count() > 0:

            raise DataQualityException(
                f"FK validation failed for {fk_column}"
            )

    @staticmethod
    def validate_non_negative(
            df: DataFrame,
            amount_column: str
    ):

        failures = (

            df.filter(
                F.col(amount_column) < 0
            )
        )

        if failures.count() > 0:

            raise DataQualityException(
                f"Negative values found in {amount_column}"
            )

    @staticmethod
    def validate_order_totals(
            orders_df,
            order_items_df
    ):

        item_totals = (

            order_items_df

            .groupBy("order_id")

            .agg(
                F.sum(
                    "line_amount"
                ).alias(
                    "calculated_total"
                )
            )
        )

        failures = (

            orders_df.alias("o")

            .join(
                item_totals.alias("i"),
                "order_id"
            )

            .filter(
                F.col("o.total_amount")
                !=
                F.col("i.calculated_total")
            )
        )

        if failures.count() > 0:

            raise DataQualityException(
                "Order totals do not match line items"
            )
        
def main():

    spark = DataQualityChecks.create_spark_session()

    dim_customer = spark.read.parquet(
        "data/warehouse/dim_customer"
    )

    dim_product = spark.read.parquet(
        "data/warehouse/dim_product"
    )

    fact_orders = spark.read.parquet(
        "data/warehouse/fact_orders"
    )

    fact_order_items = spark.read.parquet(
        "data/warehouse/fact_order_items"
    )

    fact_payments = spark.read.parquet(
        "data/warehouse/fact_payment_attempts"
    )

    #Add the Data Quality checks for the required warehouse tables
    

    DataQualityChecks.validate_pk_uniqueness(
        dim_customer,
        "customer_id"
    )

    DataQualityChecks.validate_fk(
        fact_orders,
        dim_customer,
        "customer_id"
    )

    DataQualityChecks.validate_order_totals(
        fact_orders,
        fact_order_items
    )

if __name__ == "__main__":
    main()