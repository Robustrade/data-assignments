"""
Warehouse = latest known state per row, rebuilt by replaying the lake.
No separate write path - the warehouse is always "which lake events have
been applied so far", which keeps it consistent with the lake and makes
rebuild/restore possible.

Each warehouse table mirrors its source columns plus:
  _cdc_seq  - sequence of the last lake event applied to this row
  _deleted  - soft-delete flag (history/restore still needs the row)

Last-write-wins is enforced with `WHERE excluded._cdc_seq > wh_x._cdc_seq`
on the upsert itself, so an out-of-order or duplicate event can never
clobber a row that's already ahead of it.

apply_lake_events() is storage-agnostic - it just takes a list of event
dicts, whether they came from Parquet, a Kafka microbatch, or a test. That's
what lets pipeline/spark_consumer.py reuse this exact merge logic.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

# duckdb is only needed inside reconstruct_as_of() - kept out of the
# module-level imports so this stays importable in the Spark consumer's
# environment, which doesn't have duckdb installed.
if TYPE_CHECKING:
    import duckdb

from source.models import EXPECTED_COLUMNS, PRIMARY_KEYS, COLUMN_TYPES


def _warehouse_table(source_table: str) -> str:
    return f"wh_{source_table}"


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for table, columns in EXPECTED_COLUMNS.items():
        pk = PRIMARY_KEYS[table]
        types = COLUMN_TYPES[table]
        col_defs = ", ".join(f"{c} {types[c]}" for c in columns)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_warehouse_table(table)} (
                {col_defs},
                _cdc_seq   BIGINT NOT NULL,
                _deleted   BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY ({pk})
            )
        """)


def _bind_expr(sql_type: str) -> str:
    # decimals/timestamps round-trip through JSON as plain strings, so they
    # need an explicit cast back on the way into the warehouse
    if sql_type.startswith("DECIMAL") or sql_type == "TIMESTAMP":
        return f"CAST(? AS {sql_type})"
    return "?"


def apply_lake_events(conn: duckdb.DuckDBPyConnection, lake_rows: list[dict]) -> None:
    """lake_rows must be sorted by sequence. Each `data` is a full row
    snapshot, not a diff - applying it is always a full-column overwrite."""
    for row in lake_rows:
        table = row["table_name"]
        if table not in EXPECTED_COLUMNS:
            continue  # event for a table we don't have a warehouse model for

        columns = EXPECTED_COLUMNS[table]
        types = COLUMN_TYPES[table]
        pk = PRIMARY_KEYS[table]
        data = json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
        is_delete = row["operation"] == "delete"

        insert_cols = columns + ["_cdc_seq", "_deleted"]
        values = [data.get(c) for c in columns] + [row["sequence"], is_delete]

        placeholders = ", ".join(_bind_expr(types[c]) for c in columns) + ", ?, ?"
        update_clause = ", ".join(f"{c} = excluded.{c}" for c in insert_cols)
        wh_table = _warehouse_table(table)

        conn.execute(
            f"""
            INSERT INTO {wh_table} ({", ".join(insert_cols)})
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE SET {update_clause}
            WHERE excluded._cdc_seq > {wh_table}._cdc_seq
            """,
            values,
        )


def reconstruct_as_of(lake_rows_up_to_bound: list[dict]) -> duckdb.DuckDBPyConnection:
    """Time travel: build a throwaway warehouse from lake events up to some
    bound. Never touches the live warehouse."""
    import duckdb  # see the TYPE_CHECKING note up top

    snapshot_conn = duckdb.connect(":memory:")
    create_warehouse_tables(snapshot_conn)
    apply_lake_events(snapshot_conn, lake_rows_up_to_bound)
    return snapshot_conn
