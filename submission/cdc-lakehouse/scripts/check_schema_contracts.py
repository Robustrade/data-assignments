#!/usr/bin/env python3
"""
Validate that schemas satisfy the source-team contract (CI-friendly DuckDB mirror).

Exit 0 — contract satisfied.
Exit 1 — violations found.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.schema_check import check_contract_columns
from source.models import SCHEMA_CONTRACT, create_source_tables


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    actual = {
        table: {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}
        for table in SCHEMA_CONTRACT
    }
    violations = check_contract_columns(actual)

    if violations:
        print("Schema contract violations:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print(f"All schema contracts passed ({len(SCHEMA_CONTRACT)} tables checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
