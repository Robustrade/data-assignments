#!/usr/bin/env python3
"""
demo_schema_change.py

Demonstrates the stop-the-line behavior when the source schema drifts
incompatibly from the contract.

Scenario
--------
  1. Bootstrap the source as designed.
  2. Run a normal CDC tick — succeeds.
  3. Drop a contract column from the source.
  4. Run another tick — pipeline aborts with a clear failure signal.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.cdc import CDCCapture, SchemaContractViolation
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.schema_detector import SchemaDetector
from source.models import create_source_tables
from source.seed import build_seed_rows


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    bootstrap(conn)
    capture = CDCCapture()
    detector = SchemaDetector(conn)

    print("== first tick (clean) ==")
    for row in build_seed_rows():
        capture.insert(row.table, row.pk, row.data)
    stats = run_tick(conn, capture, detector=detector)
    print(f"  stats: {stats}")

    print("== drift the source: drop customers.email ==")
    # Drop FK-dependent tables first so the ALTER works in DuckDB.
    conn.execute("DROP TABLE IF EXISTS ledger_entries")
    conn.execute("DROP TABLE IF EXISTS transfer_events")
    conn.execute("DROP TABLE IF EXISTS transfers")
    conn.execute("DROP TABLE IF EXISTS payment_methods")
    conn.execute("DROP TABLE IF EXISTS wallets")
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    print("== second tick (must abort) ==")
    stats = run_tick(conn, capture, detector=detector)
    if not stats.get("aborted"):
        print("  FAIL: pipeline did not abort on incompatible schema change")
        return 1
    print("  pipeline correctly aborted with reason:")
    print(f"    {stats.get('reason')}")
    print("  drift summary:")
    print(stats.get("drift_summary"))

    print("== capture-level stop-the-line ==")
    capture_strict = CDCCapture(detector, require_compatible_schema=True)
    try:
        capture_strict.insert(
            "customers", "c_new",
            {"customer_id": "c_new", "full_name": "X"},
        )
    except SchemaContractViolation as exc:
        print(f"  CDCCapture refused write: {exc}")
        return 0
    print("  FAIL: CDCCapture should have raised SchemaContractViolation")
    return 1


if __name__ == "__main__":
    sys.exit(main())
