#!/usr/bin/env python3
"""
run_data_quality_checks.py

Seeds the lakehouse with the representative scenario, runs the full ingest cycle, then
asserts system + business validation parity against the warehouse outputs.

Exit 0 — all checks pass.
Exit 1 — one or more failures found.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from ingestion.pipeline import build_lakehouse, run_ingestion
from ingestion.seed import seed_capture
from validation.checks import run_all_checks


def main() -> int:
    conn = duckdb.connect(":memory:")
    build_lakehouse(conn)
    run_ingestion(conn, seed_capture())

    failures = run_all_checks(conn)

    if failures:
        print("Data quality failures:")
        for f in failures:
            print(f"  x {f}")
        return 1

    print("All data quality checks passed (system + business parity).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
