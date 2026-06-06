#!/usr/bin/env python3
"""
pg_seed.py — insert a realistic set of sample rows into the Postgres source.

Idempotent (ON CONFLICT DO NOTHING) so you can run it repeatedly. After seeding,
run scripts/pg_sync.py to capture the changes into the lake + warehouse.

Invariants are kept intact so validation parity passes:
  line_total == quantity * unit_price, and order.total == sum(line_total).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres.bridge import connect_postgres

# (customer_id, name, email, status)
CUSTOMERS = [
    ("c1", "Alice Chen", "alice@example.com", "active"),
    ("c2", "Bob Martinez", "bob@example.com", "active"),
    ("c3", "Carol Nguyen", "carol@example.com", "active"),
    ("c4", "Dan OBrien", "dan@example.com", "suspended"),
    ("c5", "Eve Kowalski", "eve@example.com", "active"),
    ("c6", "Frank Muller", "frank@example.com", "closed"),
]

# (product_id, sku, name, price, status)
PRODUCTS = [
    ("p1", "SKU-KEYB", "Mechanical Keyboard", 30.00, "active"),
    ("p2", "SKU-MOUSE", "Wireless Mouse", 10.00, "active"),
    ("p3", "SKU-MON27", "27in 4K Monitor", 199.99, "active"),
    ("p4", "SKU-USBC", "USB-C Cable 2m", 12.50, "active"),
    ("p5", "SKU-CAM", "1080p Webcam", 59.00, "active"),
    ("p6", "SKU-MAT", "Desk Mat XL", 25.00, "discontinued"),
]

# (order_id, customer_id, status, currency, [(item_id, product_id, qty, unit_price)])
ORDERS = [
    ("o1", "c1", "pending", "USD", [("oi1", "p1", 2, 30.00), ("oi2", "p2", 1, 10.00)]),
    ("o2", "c3", "shipped", "USD", [("oi3", "p3", 1, 199.99), ("oi4", "p4", 2, 12.50)]),
    ("o3", "c4", "pending", "EUR", [("oi5", "p5", 1, 59.00)]),
    (
        "o4",
        "c5",
        "delivered",
        "USD",
        [("oi6", "p4", 3, 12.50), ("oi7", "p1", 1, 30.00)],
    ),
    ("o5", "c1", "cancelled", "USD", [("oi8", "p5", 2, 59.00)]),
    ("o6", "c2", "paid", "GBP", [("oi9", "p2", 4, 10.00), ("oi10", "p6", 1, 25.00)]),
]

# (payment_id, order_id, amount, method, status, settled)
PAYMENTS = [
    ("pay1", "o1", 70.00, "card", "pending", False),
    ("pay2", "o2", 224.99, "card", "settled", True),
    ("pay3", "o3", 59.00, "wallet", "pending", False),
    ("pay4", "o4", 67.50, "bank_transfer", "settled", True),
    ("pay5", "o5", 118.00, "card", "failed", False),
    ("pay6", "o6", 65.00, "wallet", "settled", True),
]


def build_rows() -> list:
    rows = []

    for cid, name, email, status in CUSTOMERS:
        rows.append(
            "INSERT INTO customers (customer_id, name, email, status) "
            f"VALUES ('{cid}','{name}','{email}','{status}') ON CONFLICT DO NOTHING"
        )

    for pid, sku, name, price, status in PRODUCTS:
        rows.append(
            "INSERT INTO products (product_id, sku, name, price, status) "
            f"VALUES ('{pid}','{sku}','{name}',{price},'{status}') "
            "ON CONFLICT DO NOTHING"
        )

    for oid, cid, status, currency, items in ORDERS:
        total = round(sum(qty * unit for _, _, qty, unit in items), 2)
        rows.append(
            "INSERT INTO orders (order_id, customer_id, status, total, currency) "
            f"VALUES ('{oid}','{cid}','{status}',{total},'{currency}') "
            "ON CONFLICT DO NOTHING"
        )
        for item_id, pid, qty, unit in items:
            line = round(qty * unit, 2)
            rows.append(
                "INSERT INTO order_items (order_item_id, order_id, product_id, "
                "quantity, unit_price, line_total) "
                f"VALUES ('{item_id}','{oid}','{pid}',{qty},{unit},{line}) "
                "ON CONFLICT DO NOTHING"
            )

    for pid, oid, amount, method, status, settled in PAYMENTS:
        if settled:
            rows.append(
                "INSERT INTO payments (payment_id, order_id, amount, method, status, "
                "created_at, settled_at) "
                f"VALUES ('{pid}','{oid}',{amount},'{method}','{status}',now(),now()) "
                "ON CONFLICT DO NOTHING"
            )
        else:
            rows.append(
                "INSERT INTO payments (payment_id, order_id, amount, method, status) "
                f"VALUES ('{pid}','{oid}',{amount},'{method}','{status}') "
                "ON CONFLICT DO NOTHING"
            )

    return rows


def main() -> int:
    rows = build_rows()
    conn = connect_postgres()
    with conn:
        with conn.cursor() as cur:
            for sql in rows:
                cur.execute(sql)
    conn.close()
    print(
        f"Seeded {len(CUSTOMERS)} customers, {len(PRODUCTS)} products, "
        f"{len(ORDERS)} orders, {len(PAYMENTS)} payments into Postgres."
    )
    print("Next: python scripts/pg_sync.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
