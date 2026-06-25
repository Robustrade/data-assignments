"""
Warehouse layer — current-state snapshot built from CDC events.

Each warehouse table mirrors the source table with two extra columns:
  _cdc_seq  : sequence of the last CDC event that touched this row
  _deleted  : soft-delete flag set when a DELETE event is received

Production analogue: BigQuery/Snowflake tables refreshed by a streaming
merge job keyed on primary key. SCD2 history tables would sit alongside
these current-state tables for time-travel queries.
"""

from __future__ import annotations

import duckdb

from pipeline.cdc import CDCRecord

# Source table → warehouse table
_TABLE_MAP: dict[str, str] = {
    "customers": "wh_customers",
    "wallets": "wh_wallets",
    "transactions": "wh_transactions",
}

# Source table → primary key column name
_PK_MAP: dict[str, str] = {
    "customers": "customer_id",
    "wallets": "wallet_id",
    "transactions": "transaction_id",
}


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
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
        CREATE TABLE IF NOT EXISTS wh_transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            wallet_id       VARCHAR,
            amount          DECIMAL(18, 2),
            direction       VARCHAR,
            status          VARCHAR,
            reference       VARCHAR,
            created_at      TIMESTAMP,
            settled_at      TIMESTAMP,
            _cdc_seq        INTEGER NOT NULL,
            _deleted        BOOLEAN NOT NULL DEFAULT false
        )
    """)


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]
) -> None:
    """
    Apply CDC records to the warehouse current-state tables in sequence order.

    - insert / update  → upsert (insert new row or overwrite existing)
    - delete           → set _deleted = true
    """
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
