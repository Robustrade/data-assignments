"""
Writes to the real source.duckdb (so its PK/FK/CHECK/NOT NULL constraints
apply) and only after that succeeds emits the matching event to Kafka -
source write first, capture second, so we never emit an event for something
that didn't actually happen in the source.
"""

from source.models import EXPECTED_COLUMNS, PRIMARY_KEYS


class SourceWriter:
    def __init__(self, source_conn, capture):
        self.source_conn = source_conn
        self.capture = capture

    def insert(self, table: str, pk: str, row: dict) -> dict:
        columns = EXPECTED_COLUMNS[table]
        placeholders = ", ".join("?" for _ in columns)
        values = [row.get(c) for c in columns]
        self.source_conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", values
        )
        return self.capture.insert(table, pk, row)

    def update(self, table: str, pk: str, row: dict) -> dict:
        columns = EXPECTED_COLUMNS[table]
        pk_col = PRIMARY_KEYS[table]
        set_cols = [c for c in columns if c != pk_col]
        set_clause = ", ".join(f"{c} = ?" for c in set_cols)
        values = [row.get(c) for c in set_cols] + [pk]
        self.source_conn.execute(f"UPDATE {table} SET {set_clause} WHERE {pk_col} = ?", values)
        return self.capture.update(table, pk, row)

    def delete(self, table: str, pk: str, row: dict) -> dict:
        # row still carries the last-known values, same as capture.delete()
        # expects - the source row is really removed, but Kafka/lake keep
        # what it looked like right before deletion
        pk_col = PRIMARY_KEYS[table]
        self.source_conn.execute(f"DELETE FROM {table} WHERE {pk_col} = ?", [pk])
        return self.capture.delete(table, pk, row)
