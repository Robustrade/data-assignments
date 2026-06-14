from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import json


class StatusTransitionException(Exception):
    pass


class StatusTransitionValidator:
    """
    Validates order status transitions from CDC events.

    Examples:

    VALID:
        CREATED -> PAID
        CREATED -> CANCELLED
        PAID -> SHIPPED
        SHIPPED -> DELIVERED

    INVALID:
        DELIVERED -> CREATED
        DELIVERED -> PAID
        CANCELLED -> PAID
    """

    ALLOWED_TRANSITIONS = {

        "CREATED": [
            "PAID",
            "CANCELLED"
        ],

        "PAID": [
            "SHIPPED"
        ],

        "SHIPPED": [
            "DELIVERED"
        ],

        "DELIVERED": [],

        "CANCELLED": []
    }

    @staticmethod
    def _extract_status(json_string):

        if json_string is None:
            return None

        try:
            payload = json.loads(json_string)

            return payload.get("order_status")

        except Exception:
            return None

    @classmethod
    def validate(
            cls,
            cdc_df: DataFrame
    ):
        """
        Validates order status transitions from CDC events.

        Expected CDC schema:

        sequence_number
        table_name
        operation
        primary_key
        before_image
        after_image
        event_timestamp
        """

        extract_before_status = F.udf(
            lambda x: cls._extract_status(x)
        )

        extract_after_status = F.udf(
            lambda x: cls._extract_status(x)
        )

        order_updates = (

            cdc_df

            .filter(
                F.col("table_name") == "orders"
            )

            .filter(
                F.col("operation") == "UPDATE"
            )

            .withColumn(
                "old_status",
                extract_before_status(
                    F.col("before_image")
                )
            )

            .withColumn(
                "new_status",
                extract_after_status(
                    F.col("after_image")
                )
            )
        )

        invalid_transitions = []

        rows = order_updates.select(
            "primary_key",
            "old_status",
            "new_status"
        ).collect()

        for row in rows:

            old_status = row["old_status"]
            new_status = row["new_status"]

            if old_status is None:
                continue

            allowed = cls.ALLOWED_TRANSITIONS.get(
                old_status,
                []
            )

            if new_status not in allowed:

                invalid_transitions.append(
                    (
                        row["primary_key"],
                        old_status,
                        new_status
                    )
                )

        if invalid_transitions:

            error_message = "\n".join(
                [
                    (
                        f"Order {order_id}: "
                        f"{old_status} -> {new_status}"
                    )
                    for (
                        order_id,
                        old_status,
                        new_status
                    ) in invalid_transitions
                ]
            )

            raise StatusTransitionException(
                f"Invalid status transitions detected:\n"
                f"{error_message}"
            )

        return True