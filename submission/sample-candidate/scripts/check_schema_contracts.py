#!/usr/bin/env python3
"""
check_schema_contracts.py

Validates that the source tables expose all columns defined in the schema
contract. A missing column means downstream CDC logic or warehouse models
will break — this script fails the build before that happens.

Exit 0 — all contracts pass.
Exit 1 — one or more violations found.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from source.models import SCHEMA_CONTRACT, create_source_tables


def check_contracts(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Return a list of violation messages. Empty list = all passed."""
    violations: list[str] = []

    for table, expected_cols in SCHEMA_CONTRACT.items():
        try:
            rows = conn.execute(f"DESCRIBE {table}").fetchall()
        except Exception as exc:
            violations.append(f"{table}: could not describe table — {exc}")
            continue

        actual_cols = {row[0] for row in rows}

        for col in expected_cols:
            if col not in actual_cols:
                violations.append(f"{table}.{col}: column missing from source table")

    return violations


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    violations = check_contracts(conn)

    if violations:
        print("Schema contract violations:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print(f"All schema contracts passed ({len(SCHEMA_CONTRACT)} tables checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
