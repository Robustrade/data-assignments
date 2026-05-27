"""
DuckDB mirror of the MySQL source schema for local tests and CI contract checks.

Production source of record: MySQL (see source/schema.sql).
"""

from __future__ import annotations

import duckdb

SCHEMA_CONTRACT: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "name",
        "email",
        "phone",
        "status",
        "created_at",
        "updated_at",
        "is_deleted",
    ],
    "products": [
        "product_id",
        "sku",
        "name",
        "unit_price",
        "created_at",
        "updated_at",
        "is_deleted",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_total",
        "created_at",
        "updated_at",
        "paid_at",
        "cancel_reason",
        "is_deleted",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "is_deleted",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "payment_amount",
        "payment_status",
        "created_at",
        "updated_at",
        "is_deleted",
    ],
    "order_status_events": [
        "status_event_id",
        "order_id",
        "from_status",
        "to_status",
        "event_at",
        "is_deleted",
    ],
}

BUSINESS_TABLES = list(SCHEMA_CONTRACT.keys())


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create in-memory source tables matching MySQL column contracts."""
    conn.execute("""
        CREATE TABLE customers (
            customer_id BIGINT PRIMARY KEY,
            name        VARCHAR NOT NULL,
            email       VARCHAR,
            phone       VARCHAR,
            status      VARCHAR NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL,
            is_deleted  BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE products (
            product_id  BIGINT PRIMARY KEY,
            sku         VARCHAR NOT NULL,
            name        VARCHAR NOT NULL,
            unit_price  DECIMAL(12, 2) NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL,
            is_deleted  BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE orders (
            order_id      BIGINT PRIMARY KEY,
            customer_id   BIGINT NOT NULL,
            order_status  VARCHAR NOT NULL,
            order_total   DECIMAL(12, 2) NOT NULL,
            created_at    TIMESTAMP NOT NULL,
            updated_at    TIMESTAMP NOT NULL,
            paid_at       TIMESTAMP,
            cancel_reason VARCHAR,
            is_deleted    BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE order_items (
            order_item_id BIGINT PRIMARY KEY,
            order_id      BIGINT NOT NULL,
            product_id    BIGINT NOT NULL,
            quantity      INTEGER NOT NULL,
            unit_price    DECIMAL(12, 2) NOT NULL,
            line_total    DECIMAL(12, 2) NOT NULL,
            is_deleted    BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE payments (
            payment_id     BIGINT PRIMARY KEY,
            order_id       BIGINT NOT NULL,
            payment_amount DECIMAL(12, 2) NOT NULL,
            payment_status VARCHAR NOT NULL,
            created_at     TIMESTAMP NOT NULL,
            updated_at     TIMESTAMP NOT NULL,
            is_deleted     BOOLEAN NOT NULL DEFAULT false
        )
    """)
    conn.execute("""
        CREATE TABLE order_status_events (
            status_event_id BIGINT PRIMARY KEY,
            order_id        BIGINT NOT NULL,
            from_status     VARCHAR,
            to_status       VARCHAR NOT NULL,
            event_at        TIMESTAMP NOT NULL,
            is_deleted      BOOLEAN NOT NULL DEFAULT false
        )
    """)
