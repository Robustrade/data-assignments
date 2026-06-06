#!/usr/bin/env python3
"""
pg_sync.py — capture Postgres changes into the lake + warehouse, then report state.

Run this after inserting/updating/deleting rows in Postgres. It diffs the source
against the last sync, writes the changes to the lake, refreshes the warehouse
(current snapshot + SCD2 history), runs validation parity, and prints a summary.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres.bridge import connect_postgres, open_lakehouse, sync
from validation.checks import run_all_checks
from warehouse.warehouse import reconstruct_as_of


def main() -> int:
    duck = open_lakehouse()
    pg = connect_postgres()
    try:
        counts = sync(pg, duck)
    finally:
        pg.close()

    print("=== sync ===")
    print(
        f"captured: {counts['insert']} insert, {counts['update']} update, "
        f"{counts['delete']} delete ({counts['applied']} applied)"
    )

    lake_total = duck.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    by_op = dict(
        duck.execute(
            "SELECT operation, COUNT(*) FROM lake_cdc_events GROUP BY operation"
        ).fetchall()
    )
    print(f"\n=== lake (full history) ===\n{lake_total} events {by_op}")

    print("\n=== warehouse current snapshot ===")
    for table in ("customers", "products", "orders", "order_items", "payments"):
        rows = duck.execute(
            f"SELECT COUNT(*) FROM wh_{table} WHERE NOT _deleted"
        ).fetchone()[0]
        print(f"  wh_{table}: {rows} live row(s)")

    orders = duck.execute(
        "SELECT order_id, status, total FROM wh_orders WHERE NOT _deleted "
        "ORDER BY order_id"
    ).fetchall()
    print(f"  orders: {orders}")

    max_seq = duck.execute(
        "SELECT COALESCE(MAX(sequence), 0) FROM lake_cdc_events"
    ).fetchone()[0]
    if max_seq:
        first = reconstruct_as_of(duck, "orders", as_of_seq=1)
        print(f"\n=== time travel ===\norders as of seq 1: {first}")
        latest = reconstruct_as_of(duck, "orders", as_of_seq=max_seq)
        print(f"orders as of seq {max_seq}: {latest}")

    failures = run_all_checks(duck)
    print("\n=== validation parity ===")
    print("  all checks passed" if not failures else f"  FAILURES: {failures}")
    duck.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
