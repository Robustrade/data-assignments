"""
Source schema for an e-commerce order-management system.

Domain: customers place orders; orders contain line items and are paid
via one or more payment attempts.

Strong entities : customers, orders
Weak entities   : order_items (lifecycle tied to its order), payments
                  (tied to an order, but can arrive/retry after order
                  creation, so it carries its own timestamps/status)

Invariants:
- orders.total_amount should reconcile with sum(order_items.quantity * unit_price)
  for that order (checked downstream, not enforceable as a single-table CHECK)
- order_items.quantity > 0, unit_price >= 0
- payments.amount > 0
- payments.paid_at >= payments.created_at when paid_at is set
- status fields restricted to known enum values
- order_items.order_id / payments.order_id should reference a real order,
  but this is NOT a DuckDB-enforced FK (see init_source_schema below for
  why - a real DuckDB engine limitation, not an oversight) - checked
  instead in the data-quality layer
"""

from __future__ import annotations

# duckdb is only needed inside init_source_schema() - kept out of the
# module-level imports so this stays importable without duckdb installed
# (e.g. in the Spark consumer's environment).

# schema-contract check diffs the live source against this to catch
# dropped/renamed columns before they break anything downstream
EXPECTED_COLUMNS: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "name",
        "email",
        "status",
        "created_at",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "status",
        "total_amount",
        "discount_code",
        "created_at",
        "updated_at",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "sku",
        "quantity",
        "unit_price",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "amount",
        "status",
        "created_at",
        "paid_at",
    ],
}

# PK per table - the warehouse layer upserts on this
PRIMARY_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}

# column -> SQL type; the warehouse layer reuses this so amounts/timestamps
# keep their real types instead of collapsing into VARCHAR via JSON
COLUMN_TYPES: dict[str, dict[str, str]] = {
    "customers": {
        "customer_id": "VARCHAR",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "status": "VARCHAR",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "orders": {
        "order_id": "VARCHAR",
        "customer_id": "VARCHAR",
        "status": "VARCHAR",
        "total_amount": "DECIMAL(18,2)",
        "discount_code": "VARCHAR",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "order_items": {
        "order_item_id": "VARCHAR",
        "order_id": "VARCHAR",
        "sku": "VARCHAR",
        "quantity": "INTEGER",
        "unit_price": "DECIMAL(18,2)",
    },
    "payments": {
        "payment_id": "VARCHAR",
        "order_id": "VARCHAR",
        "amount": "DECIMAL(18,2)",
        "status": "VARCHAR",
        "created_at": "TIMESTAMP",
        "paid_at": "TIMESTAMP",
    },
}


def init_source_schema(conn: "duckdb.DuckDBPyConnection") -> None:
    """Create source tables with constraints and indexes on the given connection."""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'suspended', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id       VARCHAR PRIMARY KEY,
            customer_id    VARCHAR NOT NULL REFERENCES customers(customer_id),
            status         VARCHAR NOT NULL
                       CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled', 'refunded')),
            total_amount   DECIMAL(18, 2) NOT NULL CHECK (total_amount >= 0),
            discount_code  VARCHAR,
            created_at     TIMESTAMP NOT NULL,
            updated_at     TIMESTAMP NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")

    # order_items.order_id / payments.order_id deliberately skip
    # `REFERENCES orders(order_id)`: DuckDB 1.5.5 can't UPDATE a table that's
    # both an FK child (orders -> customers) and FK parent (order_items/
    # payments -> orders) - any UPDATE on orders fails, even one that
    # doesn't touch order_id. orders.status changes constantly, so orders
    # has to stay updatable. Referential integrity for this link is enforced
    # in the data-quality layer instead (check_referential_integrity).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id  VARCHAR PRIMARY KEY,
            order_id       VARCHAR NOT NULL,
            sku            VARCHAR NOT NULL,
            quantity       INTEGER NOT NULL CHECK (quantity > 0),
            unit_price     DECIMAL(18, 2) NOT NULL CHECK (unit_price >= 0)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id   VARCHAR PRIMARY KEY,
            order_id     VARCHAR NOT NULL,
            amount       DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('pending', 'settled', 'failed', 'refunded')),
            created_at   TIMESTAMP NOT NULL,
            paid_at      TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id)")
