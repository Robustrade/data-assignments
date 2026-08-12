"""
Tests for Schema Contract compatibility checking and stop-the-line behavior.
"""

import duckdb
import pytest

from pipeline.schema_safety import (
    SchemaIncompatibilityError,
    assert_schema_compatible,
    check_schema_compatibility,
)


def test_valid_schema_contract_passes(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify normal compliant source tables produce no schema violations."""
    violations = check_schema_compatibility(conn)
    assert violations == []
    # Assert should complete cleanly without exception
    assert_schema_compatible(conn)


def test_missing_column_triggers_stop_the_line(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify dropping a column from source triggers SchemaIncompatibilityError."""
    # Recreate ledger_entries table missing 'balance_after' column
    conn.execute("DROP TABLE ledger_entries")
    conn.execute("""
        CREATE TABLE ledger_entries (
            entry_id       VARCHAR PRIMARY KEY,
            transaction_id VARCHAR NOT NULL,
            wallet_id      VARCHAR NOT NULL,
            entry_type     VARCHAR NOT NULL,
            amount         DECIMAL(18, 2) NOT NULL,
            created_at     TIMESTAMP NOT NULL
        )
    """)

    violations = check_schema_compatibility(conn)
    assert len(violations) >= 1
    assert any("ledger_entries.balance_after" in v for v in violations)

    with pytest.raises(SchemaIncompatibilityError, match="STOP-THE-LINE"):
        assert_schema_compatible(conn)
