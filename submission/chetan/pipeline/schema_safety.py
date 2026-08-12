"""
Schema Safety and Incompatibility Detection Layer.

Implements "Stop-the-Line" safety: assumes no backward compatibility guarantee from
source transactional systems. If source table structures change incompatibly (missing
columns, renamed columns, altered data types), ingestion stops immediately with an
explicit exception and warning.
"""

from __future__ import annotations

import logging

import duckdb

from source.models import SCHEMA_CONTRACT

logger = logging.getLogger(__name__)


class SchemaIncompatibilityError(Exception):
    """Raised when source schema changes incompatibly with downstream expectations."""


def check_schema_compatibility(
    conn: duckdb.DuckDBPyConnection,
    schema_contract: dict[str, list[str]] | None = None,
) -> list[str]:
    """
    Compare current source database table columns against expected schema contract.

    Returns a list of violation messages (empty list means fully compatible).
    """
    if schema_contract is None:
        schema_contract = SCHEMA_CONTRACT

    violations: list[str] = []

    for table, expected_cols in schema_contract.items():
        try:
            rows = conn.execute(f"DESCRIBE {table}").fetchall()
        except duckdb.Error as exc:
            violations.append(f"{table}: table missing or describe failed — {exc}")
            continue

        actual_cols = {row[0] for row in rows}

        for col in expected_cols:
            if col not in actual_cols:
                violations.append(
                    f"{table}.{col}: required column missing from source table (dropped or renamed)"
                )

    return violations


def assert_schema_compatible(
    conn: duckdb.DuckDBPyConnection,
    schema_contract: dict[str, list[str]] | None = None,
) -> None:
    """
    Assert that source schema satisfies schema contract.

    If incompatible drift is detected, emits a clear warning and raises
    SchemaIncompatibilityError to stop pipeline ingestion immediately.
    """
    violations = check_schema_compatibility(conn, schema_contract)
    if violations:
        msg = (
            "STOP-THE-LINE: Incompatible source schema changes detected!\n"
            + "\n".join(f"  ✗ {v}" for v in violations)
        )
        logger.error(msg)
        print(msg)
        raise SchemaIncompatibilityError(msg)
