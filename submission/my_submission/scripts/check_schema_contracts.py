"""
    Validates that the source tables expose all columns defined in the schema
    contract. A missing column means downstream CDC logic or warehouse models
    will break — this script fails the build before that happens.

    REQUIREMENT: Schema Change Detection and Safe Stop
    - If any column is missing, ingestion STOPS
    - Clear error message is emitted
    - Exit code 1 (failure)

    Exit 0 — all contracts pass.
    Exit 1 — one or more violations found.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckdb
from source.models import SCHEMA_CONTRACT, create_source_tables


def check_contracts(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Validate schema contracts.
    
    Returns a list of violation messages. Empty list = all passed.
    """
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
                violations.append(
                    f"  ❌ {table}.{col}: MISSING from source table"
                )

    return violations


def check_violations(conn: duckdb.DuckDBPyConnection) -> int:
    """Print violation messages and exit if any found."""
    violations = check_contracts(conn)

    if violations:
        print("⚠️  SCHEMA CONTRACT VIOLATIONS DETECTED")
        print("=" * 60)
        for v in violations:
            print(v)
        print("=" * 60)
        print("❌ Ingestion STOPPED — source schema incompatible")
        print("Action: Fix source schema or update contract")
        return 1
    print(f"✅ All schema contracts passed ({len(SCHEMA_CONTRACT)} tables checked)")
    print("   Safe to proceed with CDC ingestion")
    return 0

def main() -> int:
    """Run schema contract checks."""
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    return check_violations(conn)

if __name__ == "__main__":
    sys.exit(main())
