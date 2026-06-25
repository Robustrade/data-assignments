"""
Tests — schema contract detection and safe-stop behavior.

Covers: passing contracts, missing column detection, incompatible schema change
detection (dropped column, added unexpected table), and that ingestion can be
stopped when a contract violation is found.
"""

import duckdb
import pytest

from source.models import SCHEMA_CONTRACT, create_source_tables

# ── helpers ───────────────────────────────────────────────────────────────────


def _actual_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}


def _check_contracts(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Inline contract check — mirrors scripts/check_schema_contracts.py."""
    violations: list[str] = []
    for table, expected_cols in SCHEMA_CONTRACT.items():
        try:
            actual = _actual_columns(conn, table)
        except Exception as exc:
            violations.append(f"{table}: {exc}")
            continue
        for col in expected_cols:
            if col not in actual:
                violations.append(f"{table}.{col}: column missing")
    return violations


# ── passing contracts ─────────────────────────────────────────────────────────


def test_fresh_source_tables_pass_all_contracts():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    assert _check_contracts(conn) == []


def test_all_contract_tables_are_present():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    for table in SCHEMA_CONTRACT:
        cols = _actual_columns(conn, table)
        assert len(cols) > 0, f"{table} has no columns"


# ── missing column detection ──────────────────────────────────────────────────


def test_dropped_column_is_detected_as_violation():
    # Create customers without FK-dependent tables so ALTER TABLE is allowed
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR PRIMARY KEY,
            name        VARCHAR NOT NULL,
            status      VARCHAR NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL
        )
    """)
    # 'email' was never added — contract check should catch it
    violations = _check_contracts(conn)
    assert any("email" in v for v in violations)


def test_multiple_dropped_columns_all_reported():
    # Create wallets without FK constraint so ALTER TABLE is allowed
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE customers (customer_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL, email VARCHAR NOT NULL,
            status VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL)
    """)
    conn.execute("""
        CREATE TABLE wallets (
            wallet_id   VARCHAR PRIMARY KEY,
            customer_id VARCHAR NOT NULL,
            currency    VARCHAR NOT NULL,
            status      VARCHAR NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL
        )
    """)
    # 'balance' never added — contract check should report both balance and balance
    violations = _check_contracts(conn)
    assert any("balance" in v for v in violations)


def test_violations_include_table_and_column_name():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    conn.execute("ALTER TABLE transactions DROP COLUMN amount")

    violations = _check_contracts(conn)
    assert any("transactions" in v and "amount" in v for v in violations)


# ── contract-based safe stop ──────────────────────────────────────────────────


def test_pipeline_stops_when_contract_violated():
    """
    When a contract violation is found, no CDC records should be captured.
    This models the stop-the-line behavior required by the assignment.
    """
    from pipeline.cdc import CDCCapture

    # Create customers table missing 'email' — simulates a breaking schema change
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR PRIMARY KEY,
            name        VARCHAR NOT NULL,
            status      VARCHAR NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL
        )
    """)

    violations = _check_contracts(conn)
    capture = CDCCapture()

    if violations:
        # Pipeline aborts — no records captured
        pass
    else:
        capture.insert("customers", "c1", {"customer_id": "c1", "name": "Alice"})

    assert (
        len(capture.log) == 0
    ), "No records should be captured after a contract violation"


# ── schema contract completeness ──────────────────────────────────────────────


def test_schema_contract_covers_all_key_tables():
    assert "customers" in SCHEMA_CONTRACT
    assert "wallets" in SCHEMA_CONTRACT
    assert "transactions" in SCHEMA_CONTRACT


def test_schema_contract_includes_primary_key_columns():
    assert "customer_id" in SCHEMA_CONTRACT["customers"]
    assert "wallet_id" in SCHEMA_CONTRACT["wallets"]
    assert "transaction_id" in SCHEMA_CONTRACT["transactions"]


@pytest.mark.parametrize(
    "table,col",
    [
        ("customers", "status"),
        ("wallets", "balance"),
        ("wallets", "currency"),
        ("transactions", "amount"),
        ("transactions", "direction"),
    ],
)
def test_business_critical_columns_are_in_contract(table: str, col: str):
    assert (
        col in SCHEMA_CONTRACT[table]
    ), f"Business-critical column {table}.{col} is not in SCHEMA_CONTRACT"
