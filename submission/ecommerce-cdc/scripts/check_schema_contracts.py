#!/usr/bin/env python3
"""
check_schema_contracts.py

CI gate for schema safety. Builds the source schema and asserts it matches the
declared SCHEMA_CONTRACT. A breaking change (dropped/renamed column, type change,
nullable tightening, shrunk enum) stops the build before ingestion can drift.

Exit 0 — contract satisfied (additive-only changes are reported as warnings).
Exit 1 — incompatible change detected.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from ingestion.schema_contract import SchemaIncompatibilityError, assert_compatible
from source.models import SCHEMA_CONTRACT, create_source_tables


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    try:
        warnings = assert_compatible(conn)
    except SchemaIncompatibilityError as exc:
        print("Schema contract check FAILED:")
        print(exc)
        return 1

    for table, msgs in warnings.items():
        for msg in msgs:
            print(f"  ! {table}: {msg} (additive — allowed)")

    print(f"All schema contracts passed ({len(SCHEMA_CONTRACT)} tables checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
