"""
Warehouse layer — current-state snapshot and point-in-time time-travel recovery.

Each warehouse table mirrors a source table with two extra system tracking columns:
  _cdc_seq  : sequence number of the last CDC event that touched this row
  _deleted  : soft-delete boolean flag set when a DELETE event is received

Time Travel / Historical Reconstruction:
  `reconstruct_snapshot_at(conn, target_seq)` replays Lake CDC events up to
  `target_seq` to reconstruct exact prior warehouse state at any historical moment.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb

from pipeline.cdc import CDCRecord

# Source table → Warehouse table mapping
_TABLE_MAP: dict[str, str] = {
    "customers": "wh_customers",
    "wallets": "wh_wallets",
    "merchants": "wh_merchants",
    "transactions": "wh_transactions",
    "ledger_entries": "wh_ledger_entries",
}

# Source table → Primary key column mapping
_PK_MAP: dict[str, str] = {
    "customers": "customer_id",
    "wallets": "wallet_id",
    "merchants": "merchant_id",
    "transactions": "transaction_id",
    "ledger_entries": "entry_id",
}


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create current-state warehouse tables with system tracking columns."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR,
            email        VARCHAR,
            status       VARCHAR,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_wallets (
            wallet_id    VARCHAR PRIMARY KEY,
            customer_id  VARCHAR,
            balance      DECIMAL(18, 2),
            currency     VARCHAR,
            status       VARCHAR,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_merchants (
            merchant_id  VARCHAR PRIMARY KEY,
            name         VARCHAR,
            email        VARCHAR,
            status       VARCHAR,
            created_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            wallet_id       VARCHAR,
            merchant_id     VARCHAR,
            amount          DECIMAL(18, 2),
            fee             DECIMAL(18, 2),
            direction       VARCHAR,
            status          VARCHAR,
            reference       VARCHAR,
            created_at      TIMESTAMP,
            settled_at      TIMESTAMP,
            _cdc_seq        INTEGER NOT NULL,
            _deleted        BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_ledger_entries (
            entry_id        VARCHAR PRIMARY KEY,
            transaction_id  VARCHAR,
            wallet_id       VARCHAR,
            entry_type      VARCHAR,
            amount          DECIMAL(18, 2),
            balance_after   DECIMAL(18, 2),
            created_at      TIMESTAMP,
            _cdc_seq        INTEGER NOT NULL,
            _deleted        BOOLEAN NOT NULL DEFAULT false
        )
    """)


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]
) -> None:
    """
    Apply CDC records to the warehouse current-state tables in sequence order.

    - insert / update → upsert row (update existing or insert new)
    - delete          → mark _deleted = true
    """
    create_warehouse_tables(conn)

    for record in sorted(records, key=lambda r: r.sequence):
        wh_table = _TABLE_MAP.get(record.table)
        pk_col = _PK_MAP.get(record.table)
        if not wh_table or not pk_col:
            continue

        pk_val = record.primary_key

        if record.operation == "delete":
            conn.execute(
                f"UPDATE {wh_table} SET _deleted = true, _cdc_seq = ?"
                f" WHERE {pk_col} = ?",
                [record.sequence, pk_val],
            )
            continue

        data = {**record.data, "_cdc_seq": record.sequence, "_deleted": False}
        cols = list(data.keys())
        vals = list(data.values())
        placeholders = ", ".join(["?"] * len(vals))

        existing = conn.execute(
            f"SELECT COUNT(*) FROM {wh_table} WHERE {pk_col} = ?",
            [pk_val],
        ).fetchone()[0]

        if existing:
            set_clause = ", ".join([f"{c} = ?" for c in cols])
            conn.execute(
                f"UPDATE {wh_table} SET {set_clause} WHERE {pk_col} = ?",
                vals + [pk_val],
            )
        else:
            col_list = ", ".join(cols)
            conn.execute(
                f"INSERT INTO {wh_table} ({col_list}) VALUES ({placeholders})",
                vals,
            )


def reconstruct_snapshot_at(
    conn: duckdb.DuckDBPyConnection, target_seq: int
) -> dict[str, list[dict[str, Any]]]:
    """
    Reconstruct point-in-time warehouse state as of sequence `target_seq` by
    replaying lake CDC events up to target_seq.

    Returns a dict mapping source table name -> list of record dictionaries
    representing active non-deleted rows at target_seq.
    """
    events = conn.execute(
        """
        SELECT sequence, operation, table_name, primary_key, data
        FROM lake_cdc_events
        WHERE sequence <= ?
        ORDER BY sequence ASC
        """,
        [target_seq],
    ).fetchall()

    # Reconstruct in-memory state dictionary
    # table_name -> primary_key -> (sequence, data, is_deleted)
    state: dict[str, dict[str, tuple[int, dict, bool]]] = {}

    for seq, op, table_name, pk, data_str in events:
        if table_name not in state:
            state[table_name] = {}

        data_dict = json.loads(data_str)
        if op == "delete":
            state[table_name][pk] = (seq, data_dict, True)
        else:
            state[table_name][pk] = (seq, data_dict, False)

    # Output active non-deleted rows per table
    result: dict[str, list[dict[str, Any]]] = {}
    for table_name, pk_map in state.items():
        active_rows = []
        for pk, (seq, data_dict, is_deleted) in pk_map.items():
            if not is_deleted:
                active_rows.append(data_dict)
        result[table_name] = active_rows

    return result
