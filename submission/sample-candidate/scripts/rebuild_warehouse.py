from pipeline.warehouse import (
    CustomerDimensionBuilder,
    ProductDimensionBuilder,
    FactOrderBuilder,
    FactOrderItemBuilder,
    FactPaymentBuilder
)

from pipeline.silver import SilverLayer


class WarehouseRebuilder:

    def __init__(
            self,
            spark
    ):

        self.spark = spark

    def rebuild_until_timestamp(
            self,
            bronze_df,
            recovery_timestamp
    ):

        replay_df = (

            bronze_df

            .filter(
                bronze_df.event_timestamp
                <= recovery_timestamp
            )
        )

        silver = SilverLayer()

        customer_df = silver.build_customer_table(
            replay_df
        )

        product_df = silver.build_product_table(
            replay_df
        )

        orders_df = silver.build_orders_table(
            replay_df
        )

        order_items_df = (
            silver.build_order_items_table(
                replay_df
            )
        )

        payments_df = (
            silver.build_payment_attempt_table(
                replay_df
            )
        )

        return {
            "customers": customer_df,
            "products": product_df,
            "orders": orders_df,
            "order_items": order_items_df,
            "payments": payments_df
        }