#!/usr/bin/env python3
"""
check_schema_contracts.py

CI entry point for the schema contract check (`schema-contract-check`).

What it does
------------
1. Spins up an in-memory DuckDB
2. Creates the source tables exactly as defined in source/models.py
3. Runs the SchemaDetector against every contract
4. Exits non-zero if any BREAKING drift is found
5. Prints WARN drifts but does not fail the build for them

This is the same code path the orchestrator uses to fail the pipeline
closed at runtime — keeping the two consistent prevents "passes in CI,
fails in prod" surprises.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.schema_detector import SchemaDetector
from source.models import create_source_tables


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    report = SchemaDetector(conn).check_all()

    if report.warnings:
        print("Schema contract WARNINGS:")
        for d in report.warnings:
            print(f"  ! {d.table}.{d.column}: {d.rule} — {d.message}")

    if report.breaking:
        print("Schema contract BREAKING violations:")
        for d in report.breaking:
            print(f"  x {d.table}.{d.column}: {d.rule} — {d.message}")
        return 1

    n_tables = len(report.by_table)
    print(f"All schema contracts passed ({n_tables} tables checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
