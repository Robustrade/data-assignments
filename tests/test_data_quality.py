"""
Tests for source and warehouse system & business validation parity.
"""

from datetime import datetime, timezone

import duckdb

from pipeline.validations import run_validation_suite
from scripts.run_data_quality_checks import populate_demo_data


def test_validation_suite_passes_on_valid_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify demo populated data passes all validation suite checks."""
    populate_demo_data(conn)
    violations = run_validation_suite(conn)
    assert violations == [], f"Unexpected violations: {violations}"


def test_validation_suite_catches_negative_balance(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Verify validation suite flags negative wallet balance in warehouse."""
    populate_demo_data(conn)
    now = datetime.now(timezone.utc)

    # Insert illegal negative balance record directly into wh_wallets
    conn.execute(
        "INSERT INTO wh_wallets VALUES ('wal_neg', 'cust_101', -100.00, 'USD', 'active', ?, ?, 99, false)",
        [now, now],
    )

    violations = run_validation_suite(conn)
    assert len(violations) >= 1
    assert any(
        "wh_wallets: 1 row(s) have negative wallet balance" in v for v in violations
    )


def test_validation_suite_catches_orphaned_foreign_key(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Verify validation suite flags orphaned foreign keys."""
    populate_demo_data(conn)
    now = datetime.now(timezone.utc)

    # Insert orphaned wallet with non-existent customer_id into wh_wallets
    conn.execute(
        "INSERT INTO wh_wallets VALUES ('wal_orphan', 'non_existent_cust', 50.00, 'USD', 'active', ?, ?, 100, false)",
        [now, now],
    )

    violations = run_validation_suite(conn)
    assert len(violations) >= 1
    assert any(
        "wh_wallets.customer_id -> wh_customers.customer_id" in v for v in violations
    )
