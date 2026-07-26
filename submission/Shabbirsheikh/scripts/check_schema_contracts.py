"""
Compares whatever the source database actually looks like right now against
EXPECTED_COLUMNS / COLUMN_TYPES in source/models.py. A dropped column,
renamed column, or changed type means the contract is broken - ingestion
should stop rather than keep writing against a wrong assumption about the
schema.

A brand new column that we don't know about yet is reported too, but it's
not treated as breaking on its own - old code just won't reference it, so
nothing crashes. It's a signal the contract needs updating, not a fail.
"""

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from source.models import EXPECTED_COLUMNS, COLUMN_TYPES, init_source_schema

DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "source.duckdb"


class SchemaContractViolation(Exception):
    pass


def _live_columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return dict(rows)


def check_table(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """List of human-readable problems for this table. Empty list = compatible."""
    live = _live_columns(conn, table)
    if not live:
        return [f"{table}: table is missing entirely from the source"]

    expected_cols = EXPECTED_COLUMNS[table]
    expected_types = COLUMN_TYPES[table]
    problems = []

    for col in expected_cols:
        if col not in live:
            problems.append(f"{table}.{col}: column missing (dropped, or renamed to something we don't recognize)")
            continue
        if live[col] != expected_types[col]:
            problems.append(f"{table}.{col}: type changed - expected {expected_types[col]}, found {live[col]}")

    unexpected = sorted(set(live) - set(expected_cols))
    if unexpected:
        problems.append(f"{table}: new column(s) {unexpected} not in the contract yet (not breaking, but update EXPECTED_COLUMNS)")

    return problems


def check_all(conn: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    report = {}
    for table in EXPECTED_COLUMNS:
        problems = check_table(conn, table)
        if problems:
            report[table] = problems
    return report


def enforce(conn: duckdb.DuckDBPyConnection) -> None:
    """Raises SchemaContractViolation on any missing column or type change.
    A purely additive new column does not fail this - see check_table."""
    report = check_all(conn)
    breaking = {
        table: [p for p in problems if "not breaking" not in p]
        for table, problems in report.items()
    }
    breaking = {t: p for t, p in breaking.items() if p}

    if breaking:
        lines = [f"  - {p}" for problems in breaking.values() for p in problems]
        raise SchemaContractViolation(
            "Source schema is incompatible with the expected contract, ingestion stopped:\n"
            + "\n".join(lines)
        )


if __name__ == "__main__":
    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DEFAULT_DB_PATH))
    init_source_schema(conn)  # no-op if tables already exist

    try:
        enforce(conn)
    except SchemaContractViolation as exc:
        print(f"SCHEMA CONTRACT VIOLATION\n{exc}", file=sys.stderr)
        sys.exit(1)

    report = check_all(conn)  # anything left here is non-breaking (new columns)
    if report:
        print("Schema check passed, but with warnings:")
        for table, problems in report.items():
            for p in problems:
                print(f"  - {p}")
    else:
        print("Schema check passed - source matches the expected contract.")
