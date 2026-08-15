from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List


SOURCE_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


class SourceReader:
    """
    Reads records and schema information from the SQLite
    transactional source database.
    """

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Read all records from a source table.
        """

        if table_name not in SOURCE_TABLES:
            raise ValueError(
                f"Unsupported source table: {table_name}"
            )

        connection = self._connect()

        try:
            rows = connection.execute(
                f'SELECT * FROM "{table_name}"'
            ).fetchall()

            return [dict(row) for row in rows]

        finally:
            connection.close()

    def read_all_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Read all source tables.
        """

        return {
            table_name: self.read_table(table_name)
            for table_name in SOURCE_TABLES
        }

    def get_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Return a normalized representation of the source schema.
        """

        connection = self._connect()

        try:
            schema: Dict[str, Dict[str, Any]] = {}

            for table_name in SOURCE_TABLES:
                columns = {}

                rows = connection.execute(
                    f'PRAGMA table_info("{table_name}")'
                ).fetchall()

                for row in rows:
                    columns[row["name"]] = {
                        "type": row["type"],
                        "nullable": not bool(row["notnull"]),
                        "primary_key": bool(row["pk"]),
                    }

                schema[table_name] = columns

            return schema

        finally:
            connection.close()