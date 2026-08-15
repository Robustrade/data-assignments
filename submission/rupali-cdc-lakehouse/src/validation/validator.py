from __future__ import annotations

from typing import List, Tuple


class WarehouseValidator:
    """
    Validation layer for the current-state warehouse.

    The validator mirrors the source-system validations
    defined in DESIGN.md:

    - Primary-key uniqueness
    - Foreign-key integrity

    Validation failures are reported through the returned
    validation result.
    """

    def __init__(self, warehouse):
        self.warehouse = warehouse

    def validate_primary_key(
        self,
        table_name: str,
        primary_key: str,
    ) -> dict:
        """
        Validate that a primary-key column contains no duplicates.
        """

        result = self.warehouse.connection.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                COUNT(DISTINCT "{}") AS distinct_count
            FROM "{}"
            """.format(
                primary_key,
                table_name,
            )
        ).fetchone()

        total_count = result[0]
        distinct_count = result[1]

        duplicate_count = total_count - distinct_count

        return {
            "valid": duplicate_count == 0,
            "duplicate_count": duplicate_count,
        }

    def validate_foreign_key(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
    ) -> dict:
        """
        Validate that every non-null child foreign-key value
        exists in the parent table.
        """

        result = self.warehouse.connection.execute(
            """
            SELECT COUNT(*)
            FROM "{child_table}" child
            LEFT JOIN "{parent_table}" parent
                ON child."{child_column}" = parent."{parent_column}"
            WHERE child."{child_column}" IS NOT NULL
              AND parent."{parent_column}" IS NULL
            """.format(
                child_table=child_table,
                child_column=child_column,
                parent_table=parent_table,
                parent_column=parent_column,
            )
        ).fetchone()

        invalid_count = result[0]

        return {
            "valid": invalid_count == 0,
            "invalid_count": invalid_count,
        }

    def validate_all(
        self,
        primary_keys: List[Tuple[str, str]],
        foreign_keys: List[Tuple[str, str, str, str]],
    ) -> dict:
        """
        Run all configured warehouse validations.

        Returns an overall result plus individual validation results.
        """

        primary_key_results = []

        for table_name, primary_key in primary_keys:
            result = self.validate_primary_key(
                table_name,
                primary_key,
            )

            primary_key_results.append(
                {
                    "table": table_name,
                    "column": primary_key,
                    **result,
                }
            )

        foreign_key_results = []

        for (
            child_table,
            child_column,
            parent_table,
            parent_column,
        ) in foreign_keys:
            result = self.validate_foreign_key(
                child_table,
                child_column,
                parent_table,
                parent_column,
            )

            foreign_key_results.append(
                {
                    "child_table": child_table,
                    "child_column": child_column,
                    "parent_table": parent_table,
                    "parent_column": parent_column,
                    **result,
                }
            )

        all_primary_keys_valid = all(
            result["valid"]
            for result in primary_key_results
        )

        all_foreign_keys_valid = all(
            result["valid"]
            for result in foreign_key_results
        )

        return {
            "valid": (
                all_primary_keys_valid
                and all_foreign_keys_valid
            ),
            "primary_keys": primary_key_results,
            "foreign_keys": foreign_key_results,
        }