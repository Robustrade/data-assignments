from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import duckdb

from src.cdc.events import CDCEvent


class WarehouseWriter:
    """
    DuckDB-backed current-state warehouse.

    The warehouse maintains the latest state of each source table.

    INSERT -> creates the current record
    UPDATE -> replaces the current record
    DELETE -> removes the current record
    """

    TABLE_PRIMARY_KEYS = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id",
        "payments": "payment_id",
    }

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.connection = duckdb.connect(
            str(self.database_path)
        )

    def _primary_key_column(self, table_name: str) -> str:
        try:
            return self.TABLE_PRIMARY_KEYS[table_name]
        except KeyError:
            raise ValueError(
                "Unsupported warehouse table: {}".format(table_name)
            )

    def _ensure_table(
        self,
        table_name: str,
        record: Dict[str, Any],
    ) -> None:
        primary_key = self._primary_key_column(table_name)

        if primary_key not in record:
            raise ValueError(
                "Record is missing primary key '{}'".format(
                    primary_key
                )
            )

        columns = list(record.keys())

        column_definitions = []

        for column in columns:
            value = record[column]

            if column == primary_key:
                sql_type = "BIGINT"
            elif isinstance(value, bool):
                sql_type = "BOOLEAN"
            elif isinstance(value, int):
                sql_type = "BIGINT"
            elif isinstance(value, float):
                sql_type = "DOUBLE"
            else:
                sql_type = "VARCHAR"

            column_definitions.append(
                '"{}" {}'.format(column, sql_type)
            )

        sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
                {}
            )
        """.format(
            table_name,
            ", ".join(column_definitions),
        )

        self.connection.execute(sql)

    def apply(self, event: CDCEvent) -> None:
        """
        Apply one CDC event to the current warehouse state.
        """

        if event.operation == "INSERT":
            self._insert(event)

        elif event.operation == "UPDATE":
            self._update(event)

        elif event.operation == "DELETE":
            self._delete(event)

        else:
            raise ValueError(
                "Unsupported CDC operation: {}".format(
                    event.operation
                )
            )

    def _insert(self, event: CDCEvent) -> None:
        record = event.after

        if record is None:
            raise ValueError(
                "INSERT event must contain after state"
            )

        self._ensure_table(
            event.table_name,
            record,
        )

        columns = list(record.keys())

        column_sql = ", ".join(
            '"{}"'.format(column)
            for column in columns
        )

        placeholders = ", ".join(
            "?"
            for _ in columns
        )

        values = [
            record[column]
            for column in columns
        ]

        sql = """
            INSERT INTO "{}" ({})
            VALUES ({})
        """.format(
            event.table_name,
            column_sql,
            placeholders,
        )

        self.connection.execute(
            sql,
            values,
        )

    def _update(self, event: CDCEvent) -> None:
        record = event.after

        if record is None:
            raise ValueError(
                "UPDATE event must contain after state"
            )

        self._ensure_table(
            event.table_name,
            record,
        )

        primary_key = self._primary_key_column(
            event.table_name
        )

        primary_key_value = event.primary_key[
            primary_key
        ]

        self.connection.execute(
            'DELETE FROM "{}" WHERE "{}" = ?'.format(
                event.table_name,
                primary_key,
            ),
            [primary_key_value],
        )

        self._insert(event)

    def _delete(self, event: CDCEvent) -> None:
        primary_key = self._primary_key_column(
            event.table_name
        )

        primary_key_value = event.primary_key[
            primary_key
        ]

        self.connection.execute(
            'DELETE FROM "{}" WHERE "{}" = ?'.format(
                event.table_name,
                primary_key,
            ),
            [primary_key_value],
        )

    def get_record(
        self,
        table_name: str,
        primary_key: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Return the current record for a primary key.

        Returns None when the record does not exist.
        """

        primary_key_column = self._primary_key_column(
            table_name
        )

        if primary_key_column not in primary_key:
            raise ValueError(
                "Missing primary key '{}'".format(
                    primary_key_column
                )
            )

        try:
            result = self.connection.execute(
                'SELECT * FROM "{}" WHERE "{}" = ?'.format(
                    table_name,
                    primary_key_column,
                ),
                [primary_key[primary_key_column]],
            )
        except duckdb.CatalogException:
            return None

        row = result.fetchone()

        if row is None:
            return None

        columns = [
            description[0]
            for description in result.description
        ]

        return dict(zip(columns, row))

    def close(self) -> None:
        self.connection.close()