#!/usr/bin/env python3
"""
pg_mutate.py — apply a batch of realistic changes to the Postgres source.

Demonstrates CDC change-capture without typing SQL: updates (legal status
transitions, a price change, a customer reactivation) and a safe delete. Run this,
then run scripts/pg_sync.py to see the updates/deletes flow into the lake + warehouse.

All changes keep the warehouse valid:
- status transitions are legal (pending->paid, paid->shipped, shipped->delivered, ...)
- the deleted customer (c6) has no orders, so no referential orphans are created
- a product price change does not affect historical order totals (orders keep their
  captured unit_price), so order.total == sum(line_total) still holds

Re-running is safe but not a no-op: each UPDATE bumps updated_at, so the row genuinely
changes and is re-captured as an update on the next sync. Status stays the same, so the
status-transition check treats it as a no-change and validation still passes.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres.bridge import connect_postgres

# (description, SQL)
MUTATIONS = [
    (
        "order o3: pending -> paid",
        "UPDATE orders SET status='paid', updated_at=now() WHERE order_id='o3'",
    ),
    (
        "payment pay3: pending -> settled",
        "UPDATE payments SET status='settled', settled_at=now() "
        "WHERE payment_id='pay3'",
    ),
    (
        "order o1: pending -> paid",
        "UPDATE orders SET status='paid', updated_at=now() WHERE order_id='o1'",
    ),
    (
        "order o2: shipped -> delivered",
        "UPDATE orders SET status='delivered', updated_at=now() WHERE order_id='o2'",
    ),
    (
        "customer c4: suspended -> active (reactivated)",
        "UPDATE customers SET status='active', updated_at=now() WHERE customer_id='c4'",
    ),
    (
        "product p2: price 10.00 -> 11.50",
        "UPDATE products SET price=11.50, updated_at=now() WHERE product_id='p2'",
    ),
    (
        "customer c6: deleted (no orders -> safe)",
        "DELETE FROM customers WHERE customer_id='c6'",
    ),
]


def main() -> int:
    conn = connect_postgres()
    with conn:
        with conn.cursor() as cur:
            for desc, sql in MUTATIONS:
                cur.execute(sql)
                print(f"  applied: {desc} (rows affected: {cur.rowcount})")
    conn.close()
    print(f"\nApplied {len(MUTATIONS)} change(s) to Postgres.")
    print("Next: python scripts/pg_sync.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
