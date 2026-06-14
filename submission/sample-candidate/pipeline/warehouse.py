from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class SCD2Processor:

    def __init__(self, spark):
        self.spark = spark

    def merge_dimension(
            self,
            incoming_df: DataFrame,
            existing_df: DataFrame,
            business_key: str,
            compare_columns: list
    ) -> DataFrame:

        current_df = (
            existing_df
            .filter(F.col("is_current") == True)
        )

        join_condition = [
            current_df[business_key]
            == incoming_df[business_key]
        ]

        joined_df = (
            incoming_df.alias("new")
            .join(
                current_df.alias("old"),
                join_condition,
                "left"
            )
        )

        change_condition = None

        for col_name in compare_columns:

            condition = (
                F.col(f"new.{col_name}")
                !=
                F.col(f"old.{col_name}")
            )

            if change_condition is None:
                change_condition = condition
            else:
                change_condition = (
                    change_condition | condition
                )

        changed_rows = joined_df.filter(
            change_condition
        )

        expired_rows = (
            current_df.alias("old")
            .join(
                changed_rows.select(
                    business_key
                ).alias("chg"),
                business_key
            )
            .withColumn(
                "effective_to",
                F.current_timestamp()
            )
            .withColumn(
                "is_current",
                F.lit(False)
            )
        )

        new_versions = (
            changed_rows
            .select(
                "new.*"
            )
            .withColumn(
                "effective_from",
                F.current_timestamp()
            )
            .withColumn(
                "effective_to",
                F.lit(None)
            )
            .withColumn(
                "is_current",
                F.lit(True)
            )
        )

        unchanged_rows = (
            existing_df.alias("e")
            .join(
                changed_rows.select(
                    business_key
                ),
                business_key,
                "leftanti"
            )
        )

        final_df = (
            unchanged_rows
            .unionByName(expired_rows)
            .unionByName(new_versions)
        )

        return final_df


class CustomerDimensionBuilder:

    def __init__(self, spark):
        self.spark = spark

    def build(
            self,
            silver_customer_df,
            existing_dimension_df
    ):

        processor = SCD2Processor(
            self.spark
        )

        return processor.merge_dimension(
            incoming_df=silver_customer_df,
            existing_df=existing_dimension_df,
            business_key="customer_id",
            compare_columns=[
                "email",
                "first_name",
                "last_name",
                "phone",
                "customer_status"
            ]
        )

class ProductDimensionBuilder:

    def __init__(self, spark):
        self.spark = spark

    def build(
            self,
            silver_product_df,
            existing_dimension_df
    ):

        processor = SCD2Processor(
            self.spark
        )

        return processor.merge_dimension(
            incoming_df=silver_product_df,
            existing_df=existing_dimension_df,
            business_key="product_id",
            compare_columns=[
                "sku",
                "product_name",
                "category",
                "unit_price",
                "product_status"
            ]
        )

class FactOrderBuilder:

    @staticmethod
    def build(
            silver_orders_df
    ):

        return silver_orders_df

class FactOrderItemBuilder:

    @staticmethod
    def build(
            silver_order_items_df
    ):

        return silver_order_items_df

class FactPaymentBuilder:

    @staticmethod
    def build(
            silver_payment_df
    ):

        return silver_payment_df

class WarehouseWriter:

    def __init__(
            self,
            warehouse_base_path
    ):

        self.base_path = warehouse_base_path

    def write_dimension(
            self,
            df,
            dimension_name
    ):

        (
            df.write
            .mode("overwrite")
            .parquet(
                f"{self.base_path}/{dimension_name}"
            )
        )

    def write_fact(
            self,
            df,
            fact_name
    ):

        (
            df.write
            .mode("overwrite")
            .parquet(
                f"{self.base_path}/{fact_name}"
            )
        )