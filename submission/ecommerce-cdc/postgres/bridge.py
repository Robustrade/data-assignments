"""
PostgreSQL -> lakehouse CDC bridge (snapshot-diff capture).

On each ``sync`` we read the current state of every source table from Postgres and
compare it to the last-seen snapshot stored in DuckDB. The differences become CDC
records (insert / update / delete) that are fed through the existing lake + warehouse
machinery, with a sequence that continues monotonically across syncs.

This is the documented "simulated CDC" approach made real against Postgres. In
production this polling diff is replaced by Debezium / logical replication reading the
WAL; everything downstream (lake, warehouse, SCD2, validation) stays identical.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Tuple

import duckdb

from ingestion.cdc import CDCCapture
from ingestion.checkpoint import (
    create_checkpoint_table,
    get_checkpoint,
    set_checkpoint,
)
from lake.lake import append_to_lake, create_lake_table
from postgres.config import LAKEHOUSE_PATH, pg_dsn
from source.models import SCHEMA_CONTRACT
from warehouse.warehouse import apply_cdc_records, create_warehouse_tables

SYNC_ORDER = ["customers", "products", "orders", "order_items", "payments"]
_SNAPSHOT_TABLE = "_pg_snapshot"


def open_lakehouse() -> duckdb.DuckDBPyConnection:
    """Open (and initialize) the persistent DuckDB lakehouse."""
    os.makedirs(os.path.dirname(LAKEHOUSE_PATH), exist_ok=True)
    conn = duckdb.connect(LAKEHOUSE_PATH)
    create_lake_table(conn)
    create_warehouse_tables(conn)
    create_checkpoint_table(conn)
    _create_snapshot_table(conn)
    return conn


def connect_postgres():
    import psycopg  # imported lazily so the core stays dependency-free

    return psycopg.connect(pg_dsn())


def sync(pg_conn, duck_conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Capture Postgres changes since the last sync into the lake + warehouse."""
    # Restart marker shared with the simulated pipeline: resume sequencing from the
    # last committed checkpoint. In steady state this equals MAX(lake sequence); we
    # guard with that max so a crash between lake-write and checkpoint can't reuse
    # a sequence (lake append is also idempotent on sequence).
    lake_max = duck_conn.execute(
        "SELECT COALESCE(MAX(sequence), 0) FROM lake_cdc_events"
    ).fetchone()[0]
    start_seq = max(int(get_checkpoint(duck_conn)), int(lake_max))
    cap = CDCCapture(start_sequence=start_seq)
    counts = {"insert": 0, "update": 0, "delete": 0}

    for table in SYNC_ORDER:
        contract = SCHEMA_CONTRACT[table]
        pk = contract.primary_key
        current = _read_pg_table(pg_conn, table, contract.column_names, pk)
        previous = _read_snapshot(duck_conn, table)

        for key, (row, row_json) in current.items():
            if key not in previous:
                cap.insert(table, key, row)
                counts["insert"] += 1
            elif previous[key] != row_json:
                cap.update(table, key, row, before=json.loads(previous[key]))
                counts["update"] += 1

        for key, old_json in previous.items():
            if key not in current:
                cap.delete(table, key, before=json.loads(old_json))
                counts["delete"] += 1

    records = cap.records_since(start_seq)
    if records:
        append_to_lake(duck_conn, records)
        apply_cdc_records(duck_conn, records)
        set_checkpoint(duck_conn, cap.latest_sequence)
        _store_snapshot(duck_conn, current_by_table=_snapshot_state(pg_conn))

    counts["applied"] = len(records)
    return counts


# -- internal helpers ---------------------------------------------------------


def _read_pg_table(
    pg_conn, table: str, columns: list, pk: str
) -> Dict[str, Tuple[Dict[str, Any], str]]:
    col_list = ", ".join(columns)
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT {col_list} FROM {table}")
        rows = cur.fetchall()
    result: Dict[str, Tuple[Dict[str, Any], str]] = {}
    for row in rows:
        record = dict(zip(columns, row))
        result[str(record[pk])] = (record, _row_json(record))
    return result


def _snapshot_state(pg_conn) -> Dict[str, Dict[str, str]]:
    state: Dict[str, Dict[str, str]] = {}
    for table in SYNC_ORDER:
        contract = SCHEMA_CONTRACT[table]
        rows = _read_pg_table(
            pg_conn, table, contract.column_names, contract.primary_key
        )
        state[table] = {pk: row_json for pk, (_, row_json) in rows.items()}
    return state


def _create_snapshot_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_SNAPSHOT_TABLE} (
            table_name VARCHAR NOT NULL,
            pk         VARCHAR NOT NULL,
            row_json   VARCHAR NOT NULL,
            PRIMARY KEY (table_name, pk)
        )
        """
    )


def _read_snapshot(conn: duckdb.DuckDBPyConnection, table: str) -> Dict[str, str]:
    rows = conn.execute(
        f"SELECT pk, row_json FROM {_SNAPSHOT_TABLE} WHERE table_name = ?",
        [table],
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def _store_snapshot(
    conn: duckdb.DuckDBPyConnection, current_by_table: Dict[str, Dict[str, str]]
) -> None:
    conn.execute(f"DELETE FROM {_SNAPSHOT_TABLE}")
    for table, rows in current_by_table.items():
        for pk, row_json in rows.items():
            conn.execute(
                f"INSERT INTO {_SNAPSHOT_TABLE} VALUES (?, ?, ?)",
                [table, pk, row_json],
            )


def _row_json(record: Dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, default=_json_default)


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    return str(obj)
