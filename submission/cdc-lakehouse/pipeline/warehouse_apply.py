"""DuckDB warehouse current-state tables driven by CDC events."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import duckdb

from pipeline.cdc_event import CDCEvent

TABLE_MAP: dict[str, str] = {
    "customers": "wh_customers",
    "products": "wh_products",
    "orders": "wh_orders",
    "order_items": "wh_order_items",
    "payments": "wh_payments",
    "order_status_events": "wh_order_status_events",
}

PK_MAP: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "order_status_events": "status_event_id",
}


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_customers (
            customer_id BIGINT PRIMARY KEY,
            name VARCHAR, email VARCHAR, phone VARCHAR, status VARCHAR,
            created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_products (
            product_id BIGINT PRIMARY KEY,
            sku VARCHAR, name VARCHAR, unit_price DECIMAL(12,2),
            created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_orders (
            order_id BIGINT PRIMARY KEY,
            customer_id BIGINT, order_status VARCHAR, order_total DECIMAL(12,2),
            created_at TIMESTAMP, updated_at TIMESTAMP, paid_at TIMESTAMP,
            cancel_reason VARCHAR, is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_order_items (
            order_item_id BIGINT PRIMARY KEY,
            order_id BIGINT, product_id BIGINT, quantity INTEGER,
            unit_price DECIMAL(12,2), line_total DECIMAL(12,2), is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_payments (
            payment_id BIGINT PRIMARY KEY,
            order_id BIGINT, payment_amount DECIMAL(12,2), payment_status VARCHAR,
            created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_order_status_events (
            status_event_id BIGINT PRIMARY KEY,
            order_id BIGINT, from_status VARCHAR, to_status VARCHAR,
            event_at TIMESTAMP, is_deleted BOOLEAN,
            _cdc_event_id BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)


def truncate_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for wh_table in TABLE_MAP.values():
        conn.execute(f"DELETE FROM {wh_table}")


def apply_events(conn: duckdb.DuckDBPyConnection, events: list[CDCEvent]) -> None:
    for event in sorted(events, key=lambda e: e.event_id):
        _apply_one(conn, event)


def _apply_one(conn: duckdb.DuckDBPyConnection, event: CDCEvent) -> None:
    wh_table = TABLE_MAP.get(event.source_table)
    pk_col = PK_MAP.get(event.source_table)
    if not wh_table or not pk_col:
        return

    pk_val = event.primary_key[pk_col]

    if event.op == "D":
        conn.execute(
            f"""
            UPDATE {wh_table}
            SET _deleted = true, _cdc_event_id = ?
            WHERE {pk_col} = ? AND (_cdc_event_id IS NULL OR _cdc_event_id < ?)
            """,
            [event.event_id, pk_val, event.event_id],
        )
        existing = conn.execute(
            f"SELECT COUNT(*) FROM {wh_table} WHERE {pk_col} = ?", [pk_val]
        ).fetchone()[0]
        if existing == 0:
            row = {pk_col: pk_val, "_cdc_event_id": event.event_id, "_deleted": True}
            _insert_row(conn, wh_table, row)
        return

    data = _normalize_row(event.row)
    data[pk_col] = pk_val
    data["_cdc_event_id"] = event.event_id
    data["_deleted"] = False

    current_seq = conn.execute(
        f"SELECT _cdc_event_id FROM {wh_table} WHERE {pk_col} = ?", [pk_val]
    ).fetchone()
    if current_seq and current_seq[0] is not None and current_seq[0] >= event.event_id:
        return

    if conn.execute(
        f"SELECT COUNT(*) FROM {wh_table} WHERE {pk_col} = ?", [pk_val]
    ).fetchone()[0]:
        _update_row(conn, wh_table, pk_col, data)
    else:
        _insert_row(conn, wh_table, data)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value
        elif isinstance(value, (dict, list)):
            out[key] = json.dumps(value)
        else:
            out[key] = value
    return out


def _insert_row(
    conn: duckdb.DuckDBPyConnection, table: str, data: dict[str, Any]
) -> None:
    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
        [data[c] for c in cols],
    )


def _update_row(
    conn: duckdb.DuckDBPyConnection, table: str, pk_col: str, data: dict[str, Any]
) -> None:
    pk_val = data[pk_col]
    set_cols = [c for c in data if c != pk_col]
    set_clause = ", ".join(f"{c} = ?" for c in set_cols)
    conn.execute(
        f"UPDATE {table} SET {set_clause} WHERE {pk_col} = ?",
        [data[c] for c in set_cols] + [pk_val],
    )
