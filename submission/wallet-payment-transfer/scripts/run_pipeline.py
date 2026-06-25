#!/usr/bin/env python3
"""
run_pipeline.py

End-to-end demo: bootstrap source + lake + warehouse, seed data, replay,
print summary. Intended as a runnable smoke test.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.schema_detector import SchemaDetector
from source.models import create_source_tables
from source.seed import build_seed_rows


def _print_summary(conn: duckdb.DuckDBPyConnection) -> None:
    n_lake = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    n_cust = conn.execute(
        "SELECT COUNT(*) FROM wh_customers WHERE _deleted = FALSE"
    ).fetchone()[0]
    n_wallets = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets WHERE _deleted = FALSE"
    ).fetchone()[0]
    n_transfers = conn.execute(
        "SELECT COUNT(*) FROM wh_transfers WHERE _deleted = FALSE"
    ).fetchone()[0]
    print(f"  lake_cdc_events  : {n_lake} events")
    print(f"  wh_customers     : {n_cust} active")
    print(f"  wh_wallets       : {n_wallets} active")
    print(f"  wh_transfers     : {n_transfers} active")


def main() -> int:
    print("== bootstrap ==")
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    bootstrap(conn)
    capture = CDCCapture()

    print("== seed ==")
    for row in build_seed_rows():
        capture.insert(row.table, row.pk, row.data)
    print(f"  captured {capture.latest_sequence} CDC records")

    print("== run tick ==")
    detector = SchemaDetector(conn)
    stats = run_tick(conn, capture, detector=detector)
    print(f"  stats: {stats}")

    print("== summary ==")
    _print_summary(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
