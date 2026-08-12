"""
Tests for source relational data models, constraints, and invariants.
"""

from datetime import datetime, timezone

import duckdb
import pytest

from source.models import SCHEMA_CONTRACT


def test_source_tables_creation(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify all 5 source tables exist and match schema contract."""
    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    assert "customers" in tables
    assert "wallets" in tables
    assert "merchants" in tables
    assert "transactions" in tables
    assert "ledger_entries" in tables


def test_schema_contract_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify expected columns exist in source tables according to SCHEMA_CONTRACT."""
    for table, expected_cols in SCHEMA_CONTRACT.items():
        actual_cols = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
        for col in expected_cols:
            assert col in actual_cols, f"Column {col} missing from {table}"


def test_wallet_negative_balance_constraint(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify wallet balance CHECK constraint rejects negative amounts."""
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO customers VALUES ('cust_1', 'Test', 't@t.com', 'active', ?, ?)",
        [now, now],
    )

    with pytest.raises(duckdb.ConstraintException):
        conn.execute(
            "INSERT INTO wallets VALUES ('wal_neg', 'cust_1', -50.00, 'USD', 'active', ?, ?)",
            [now, now],
        )


def test_transaction_status_enum_constraint(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify transaction status CHECK constraint rejects invalid status enums."""
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO customers VALUES ('cust_2', 'Test 2', 't2@t.com', 'active', ?, ?)",
        [now, now],
    )
    conn.execute(
        "INSERT INTO wallets VALUES ('wal_2', 'cust_2', 100.00, 'USD', 'active', ?, ?)",
        [now, now],
    )

    with pytest.raises(duckdb.ConstraintException):
        conn.execute(
            "INSERT INTO transactions VALUES ('tx_bad', 'wal_2', NULL, 10.00, 0.00, 'debit', 'INVALID_STATUS', NULL, ?, NULL)",
            [now],
        )
