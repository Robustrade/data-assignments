from __future__ import annotations

from typing import Any, Dict


class SchemaSafety:
    """
    Validates the current source schema against the expected schema.

    Returns True when the schemas are compatible.
    Returns False when an incompatible schema change is detected.
    """

    def __init__(self, expected_schema: Dict[str, Any]):
        self.expected_schema = expected_schema

    def validate(
        self,
        current_schema: Dict[str, Any],
    ) -> bool:
        """
        Validate source schema compatibility.

        Incompatible changes include:
        - missing tables
        - new tables
        - missing columns
        - new columns
        - changed data types
        - changed nullable requirements
        - changed primary-key definitions
        """

        expected_tables = set(self.expected_schema.keys())
        current_tables = set(current_schema.keys())

        # Missing or unexpected tables
        if expected_tables != current_tables:
            return False

        for table_name in expected_tables:
            expected_columns = self.expected_schema[table_name]
            current_columns = current_schema[table_name]

            expected_column_names = set(expected_columns.keys())
            current_column_names = set(current_columns.keys())

            # Missing or new columns
            if expected_column_names != current_column_names:
                return False

            for column_name in expected_column_names:
                expected_definition = expected_columns[column_name]
                current_definition = current_columns[column_name]

                # Data type changed
                if (
                    expected_definition.get("type")
                    != current_definition.get("type")
                ):
                    return False

                # Nullable requirement changed
                if (
                    expected_definition.get("nullable")
                    != current_definition.get("nullable")
                ):
                    return False

                # Primary key definition changed
                if (
                    expected_definition.get("primary_key", False)
                    != current_definition.get("primary_key", False)
                ):
                    return False

        return True