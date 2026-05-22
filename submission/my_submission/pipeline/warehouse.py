"""
    Warehouse layer - current-state snapshot tables built from CDC events.

    Each warehouse table mirrors the source table structure with two extra columns:
    _cdc_seq  : sequence of the last CDC event that touched this row
    _deleted  : soft-delete flag set when a DELETE event is received

    The lake is the historical source of truth. Point-in-time recovery works by
    replaying immutable lake CDC events up to a timestamp or sequence number.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
import duckdb

from pipeline.cdc import CDCRecord


_TABLE_MAP: dict[str, str] = {
    "customers": "wh_customers",
    "products": "wh_products",
    "orders": "wh_orders",
    "order_items": "wh_order_items",
    "inventory_logs": "wh_inventory_logs",
    "shipments": "wh_shipments",
    "returns": "wh_returns",
}

_PK_MAP: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "inventory_logs": "log_id",
    "shipments": "shipment_id",
    "returns": "return_id",
}

_WAREHOUSE_TABLES = list(_TABLE_MAP.values())


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all warehouse current-state snapshot tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR,
            email        VARCHAR,
            country      VARCHAR,
            status       VARCHAR,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_products (
            product_id   VARCHAR PRIMARY KEY,
            sku          VARCHAR,
            name         VARCHAR,
            category     VARCHAR,
            price        DECIMAL(18, 2),
            stock_qty    INT,
            status       VARCHAR,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_orders (
            order_id     VARCHAR PRIMARY KEY,
            customer_id  VARCHAR,
            order_date   DATE,
            status       VARCHAR,
            total_amount DECIMAL(18, 2),
            currency     VARCHAR,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_order_items (
            order_item_id  VARCHAR PRIMARY KEY,
            order_id       VARCHAR,
            product_id     VARCHAR,
            quantity       INT,
            unit_price     DECIMAL(18, 2),
            line_total     DECIMAL(18, 2),
            created_at     TIMESTAMP,
            _cdc_seq       INTEGER NOT NULL,
            _deleted       BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_inventory_logs (
            log_id           VARCHAR PRIMARY KEY,
            product_id       VARCHAR,
            transaction_type VARCHAR,
            qty_change       INT,
            reason           VARCHAR,
            created_at       TIMESTAMP,
            _cdc_seq         INTEGER NOT NULL,
            _deleted         BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_shipments (
            shipment_id  VARCHAR PRIMARY KEY,
            order_id     VARCHAR,
            status       VARCHAR,
            carrier      VARCHAR,
            tracking_num VARCHAR,
            shipped_at   TIMESTAMP,
            delivered_at TIMESTAMP,
            created_at   TIMESTAMP,
            _cdc_seq     INTEGER NOT NULL,
            _deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_returns (
            return_id     VARCHAR PRIMARY KEY,
            order_item_id VARCHAR,
            reason        VARCHAR,
            status        VARCHAR,
            refund_amount DECIMAL(18, 2),
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP,
            _cdc_seq      INTEGER NOT NULL,
            _deleted      BOOLEAN NOT NULL DEFAULT false
        )
    """)


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]
) -> None:
    """
        Apply CDC records to warehouse current-state tables.

        Processing logic:
        - insert / update -> upsert current-state by primary key
        - delete          -> set _deleted=true without removing the row
        """
    for record in sorted(records, key=lambda r: r.sequence):
        wh_table = _TABLE_MAP.get(record.table)
        pk_col = _PK_MAP.get(record.table)
        if not wh_table or not pk_col:
            continue

        if record.operation == "delete":
            conn.execute(
                f"UPDATE {wh_table} SET _deleted = true, _cdc_seq = ? WHERE {pk_col} = ?",
                [record.sequence, record.primary_key],
            )
            continue

        cols = list(record.data.keys()) + ["_cdc_seq", "_deleted"]
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        values = list(record.data.values()) + [record.sequence, False]

        conn.execute(f"DELETE FROM {wh_table} WHERE {pk_col} = ?", [record.primary_key])
        conn.execute(
            f"INSERT INTO {wh_table} ({col_names}) VALUES ({placeholders})",
            values,
        )


def get_state_at_point_in_time(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    point_in_time: datetime,
) -> list[dict[str, Any]]:
    """Reconstruct one warehouse table as of a timestamp by replaying lake CDC."""
    source_table = _source_table_for_warehouse(table_name)
    records = _read_lake_records(
        conn, table_name=source_table, through_timestamp=point_in_time
    )
    return _records_to_state(records)


def restore_warehouse_to_sequence(
    conn: duckdb.DuckDBPyConnection,
    target_sequence: int,
) -> None:
    """Restore current-state warehouse tables by replaying lake events."""
    for wh_table in _WAREHOUSE_TABLES:
        conn.execute(f"DELETE FROM {wh_table}")

    records = _read_lake_records(conn, through_sequence=target_sequence)
    apply_cdc_records(conn, records)


def get_current_state_table(
    conn: duckdb.DuckDBPyConnection, wh_table: str
) -> list[dict[str, Any]]:
    """Retrieve current active rows from a warehouse table."""
    rows = conn.execute(
        f"SELECT * FROM {wh_table} WHERE _deleted = false ORDER BY _cdc_seq DESC"
    ).fetchall()

    cols = [desc[0] for desc in conn.description]
    return [dict(zip(cols, row)) for row in rows]


def _source_table_for_warehouse(table_name: str) -> str:
    for source_table, wh_table in _TABLE_MAP.items():
        if table_name == wh_table:
            return source_table
    if table_name in _TABLE_MAP:
        return table_name
    raise ValueError(f"Unknown warehouse table: {table_name}")


def _read_lake_records(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str | None = None,
    through_sequence: int | None = None,
    through_timestamp: datetime | None = None,
) -> list[CDCRecord]:
    """Read CDCRecord objects back from the lake for replay."""
    clauses: list[str] = []
    params: list[Any] = []

    if table_name is not None:
        clauses.append("table_name = ?")
        params.append(table_name)
    if through_sequence is not None:
        clauses.append("sequence <= ?")
        params.append(through_sequence)
    if through_timestamp is not None:
        clauses.append("captured_at <= ?")
        params.append(through_timestamp)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        "SELECT sequence, operation, table_name, primary_key, data, captured_at "
        f"FROM lake_cdc_events {where} ORDER BY sequence",
        params,
    ).fetchall()

    records: list[CDCRecord] = []
    for sequence, operation, table, primary_key, data, captured_at in rows:
        records.append(
            CDCRecord(
                operation=operation,
                table=table,
                primary_key=primary_key,
                data=json.loads(data),
                captured_at=captured_at,
                sequence=sequence,
            )
        )
    return records


def _records_to_state(records: list[CDCRecord]) -> list[dict[str, Any]]:
    """Fold CDC records into latest non-deleted row state."""
    state: dict[str, dict[str, Any]] = {}

    for record in sorted(records, key=lambda r: r.sequence):
        if record.operation == "delete":
            state.pop(record.primary_key, None)
            continue

        row = dict(record.data)
        row["_cdc_seq"] = record.sequence
        row["_deleted"] = False
        state[record.primary_key] = row

    return list(state.values())
