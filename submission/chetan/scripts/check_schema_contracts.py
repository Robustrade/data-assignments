#!/usr/bin/env python3
"""
check_schema_contracts.py

Validates that source transactional database tables expose all columns declared in
the SCHEMA_CONTRACT. If any required column is missing, dropped, or renamed, the check
fails the build to prevent silent ingestion breakage or corruption downstream.

Exit 0 — schema contracts satisfied.
Exit 1 — one or more schema contract violations detected.
"""

from __future__ import annotations

import os
import sys

# Ensure workspace root and submission paths are available
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import duckdb

from pipeline.schema_safety import check_schema_compatibility
from source.models import SCHEMA_CONTRACT, create_source_tables


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    violations = check_schema_compatibility(conn, SCHEMA_CONTRACT)

    if violations:
        print("Schema contract violations detected:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print(
        f"All schema contracts passed successfully ({len(SCHEMA_CONTRACT)} tables verified)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
