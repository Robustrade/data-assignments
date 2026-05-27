#!/usr/bin/env python3
"""Run warehouse data quality checks (system + business parity)."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.config import WAREHOUSE_PATH
from pipeline.warehouse_apply import create_warehouse_tables


def _check(name: str, sql: str, conn: duckdb.DuckDBPyConnection) -> list[str]:
    count = conn.execute(sql).fetchone()[0]
    if count:
        return [f"{name}: {count} violation(s)"]
    return []


def run_checks(conn: duckdb.DuckDBPyConnection) -> list[str]:
    violations: list[str] = []

    # System: PK uniqueness (implicit via PK, but check active rows)
    violations.extend(
        _check(
            "duplicate_active_customers",
            "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM wh_customers WHERE NOT _deleted",
            conn,
        )
    )

    # FK: order_items → orders
    violations.extend(
        _check(
            "orphan_order_items",
            """
            SELECT COUNT(*) FROM wh_order_items oi
            LEFT JOIN wh_orders o ON oi.order_id = o.order_id AND NOT o._deleted
            WHERE NOT oi._deleted AND o.order_id IS NULL
            """,
            conn,
        )
    )

    # Business: non-negative money
    violations.extend(
        _check(
            "negative_order_totals",
            "SELECT COUNT(*) FROM wh_orders WHERE NOT _deleted AND order_total < 0",
            conn,
        )
    )
    violations.extend(
        _check(
            "negative_line_totals",
            "SELECT COUNT(*) FROM wh_order_items WHERE NOT _deleted AND line_total < 0",
            conn,
        )
    )

    # Business: order_total vs line items (stored total must match sum of lines)
    violations.extend(
        _check(
            "order_total_mismatch",
            """
            SELECT COUNT(*) FROM wh_orders o
            JOIN (
                SELECT order_id, SUM(line_total) AS items_total
                FROM wh_order_items
                WHERE NOT _deleted
                GROUP BY order_id
            ) x ON o.order_id = x.order_id
            WHERE NOT o._deleted AND ABS(o.order_total - x.items_total) > 0.01
            """,
            conn,
        )
    )

    # Business: paid_at after created_at
    violations.extend(
        _check(
            "paid_before_created",
            """
            SELECT COUNT(*) FROM wh_orders
            WHERE NOT _deleted AND paid_at IS NOT NULL AND paid_at < created_at
            """,
            conn,
        )
    )

    return violations


def main() -> int:
    if not WAREHOUSE_PATH.exists():
        # CI: create empty warehouse tables so script exits cleanly
        WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(WAREHOUSE_PATH))
        create_warehouse_tables(conn)
        conn.close()
        print("Warehouse not populated yet; no violations in empty tables.")
        return 0

    conn = duckdb.connect(str(WAREHOUSE_PATH))
    violations = run_checks(conn)
    conn.close()

    if violations:
        print("Data quality violations:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print("All data quality checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
