"""
Source schema for an e-commerce order system.

Domain: customers place orders; orders contain order_items (referencing products)
and are paid via payments.

Strong entities : customers, products, orders
Weak entities   : order_items (identity completed by its parent order),
                  payments     (lifecycle bound to its parent order)

The runnable pipeline materializes these tables in DuckDB so the whole solution is
dependency-free. ``source/postgres_ddl.sql`` holds the canonical PostgreSQL design
(constraints + indexes) that this mirrors.

Invariants:
- order.total == sum(order_items.line_total) for the order
- line_total == quantity * unit_price
- monetary amounts >= 0; price > 0; quantity > 0
- settled_at >= created_at when present
- enum/status fields restricted to known domains
- valid status transitions only
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import duckdb


@dataclass(frozen=True)
class ColumnSpec:
    """A single column's contract: logical type, nullability, optional enum domain."""

    name: str
    dtype: str
    nullable: bool = False
    enum: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class TableContract:
    """Declared shape of a source table used for schema-drift detection."""

    primary_key: str
    columns: List[ColumnSpec]
    foreign_keys: Dict[str, Tuple[str, str]] = field(default_factory=dict)

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Optional[ColumnSpec]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def enum_domains(self) -> Dict[str, Tuple[str, ...]]:
        return {c.name: c.enum for c in self.columns if c.enum is not None}


# Allowed business status transitions (used by validation parity).
ORDER_STATUS_TRANSITIONS: Dict[str, Tuple[str, ...]] = {
    "pending": ("paid", "cancelled"),
    "paid": ("shipped", "cancelled", "refunded"),
    "shipped": ("delivered",),
    "delivered": (),
    "cancelled": (),
    "refunded": (),
}

PAYMENT_STATUS_TRANSITIONS: Dict[str, Tuple[str, ...]] = {
    "pending": ("settled", "failed"),
    "settled": ("refunded",),
    "failed": (),
    "refunded": (),
}


SCHEMA_CONTRACT: Dict[str, TableContract] = {
    "customers": TableContract(
        primary_key="customer_id",
        columns=[
            ColumnSpec("customer_id", "VARCHAR"),
            ColumnSpec("name", "VARCHAR"),
            ColumnSpec("email", "VARCHAR"),
            ColumnSpec(
                "status",
                "VARCHAR",
                enum=("active", "suspended", "closed"),
            ),
            ColumnSpec("created_at", "TIMESTAMP"),
            ColumnSpec("updated_at", "TIMESTAMP"),
        ],
    ),
    "products": TableContract(
        primary_key="product_id",
        columns=[
            ColumnSpec("product_id", "VARCHAR"),
            ColumnSpec("sku", "VARCHAR"),
            ColumnSpec("name", "VARCHAR"),
            ColumnSpec("price", "DECIMAL(18,2)"),
            ColumnSpec("status", "VARCHAR", enum=("active", "discontinued")),
            ColumnSpec("created_at", "TIMESTAMP"),
            ColumnSpec("updated_at", "TIMESTAMP"),
        ],
    ),
    "orders": TableContract(
        primary_key="order_id",
        foreign_keys={"customer_id": ("customers", "customer_id")},
        columns=[
            ColumnSpec("order_id", "VARCHAR"),
            ColumnSpec("customer_id", "VARCHAR"),
            ColumnSpec(
                "status",
                "VARCHAR",
                enum=(
                    "pending",
                    "paid",
                    "shipped",
                    "delivered",
                    "cancelled",
                    "refunded",
                ),
            ),
            ColumnSpec("total", "DECIMAL(18,2)"),
            ColumnSpec("currency", "VARCHAR"),
            ColumnSpec("created_at", "TIMESTAMP"),
            ColumnSpec("updated_at", "TIMESTAMP"),
        ],
    ),
    "order_items": TableContract(
        primary_key="order_item_id",
        foreign_keys={
            "order_id": ("orders", "order_id"),
            "product_id": ("products", "product_id"),
        },
        columns=[
            ColumnSpec("order_item_id", "VARCHAR"),
            ColumnSpec("order_id", "VARCHAR"),
            ColumnSpec("product_id", "VARCHAR"),
            ColumnSpec("quantity", "INTEGER"),
            ColumnSpec("unit_price", "DECIMAL(18,2)"),
            ColumnSpec("line_total", "DECIMAL(18,2)"),
        ],
    ),
    "payments": TableContract(
        primary_key="payment_id",
        foreign_keys={"order_id": ("orders", "order_id")},
        columns=[
            ColumnSpec("payment_id", "VARCHAR"),
            ColumnSpec("order_id", "VARCHAR"),
            ColumnSpec("amount", "DECIMAL(18,2)"),
            ColumnSpec(
                "method",
                "VARCHAR",
                enum=("card", "wallet", "bank_transfer"),
            ),
            ColumnSpec(
                "status",
                "VARCHAR",
                enum=("pending", "settled", "failed", "refunded"),
            ),
            ColumnSpec("created_at", "TIMESTAMP"),
            ColumnSpec("settled_at", "TIMESTAMP", nullable=True),
        ],
    ),
}


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the source tables (with constraints) in the given DuckDB connection."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'suspended', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id   VARCHAR PRIMARY KEY,
            sku          VARCHAR NOT NULL,
            name         VARCHAR NOT NULL,
            price        DECIMAL(18, 2) NOT NULL CHECK (price > 0),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'discontinued')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id     VARCHAR PRIMARY KEY,
            customer_id  VARCHAR NOT NULL REFERENCES customers(customer_id),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('pending', 'paid', 'shipped',
                                       'delivered', 'cancelled', 'refunded')),
            total        DECIMAL(18, 2) NOT NULL CHECK (total >= 0),
            currency     VARCHAR NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id  VARCHAR PRIMARY KEY,
            order_id       VARCHAR NOT NULL REFERENCES orders(order_id),
            product_id     VARCHAR NOT NULL REFERENCES products(product_id),
            quantity       INTEGER NOT NULL CHECK (quantity > 0),
            unit_price     DECIMAL(18, 2) NOT NULL CHECK (unit_price >= 0),
            line_total     DECIMAL(18, 2) NOT NULL CHECK (line_total >= 0)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id   VARCHAR PRIMARY KEY,
            order_id     VARCHAR NOT NULL REFERENCES orders(order_id),
            amount       DECIMAL(18, 2) NOT NULL CHECK (amount >= 0),
            method       VARCHAR NOT NULL
                     CHECK (method IN ('card', 'wallet', 'bank_transfer')),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('pending', 'settled', 'failed', 'refunded')),
            created_at   TIMESTAMP NOT NULL,
            settled_at   TIMESTAMP
        )
        """
    )

    _create_indexes(conn)


def _create_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Indexes on FK and change-capture (updated_at) columns."""
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_updated ON orders(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_customers_updated ON customers(updated_at)",
    ]
    for stmt in statements:
        conn.execute(stmt)
