"""
Tests — source schema and constraint correctness.

Covers system invariants enforced by the source DDL:
  - PK uniqueness via duplicate insert rejection
  - NOT NULL enforcement
  - CHECK constraint enforcement (enums, non-negative balance,
    positive amount, signed_amount consistency, settled_at >= created_at,
    no self-transfer)
  - FK referential integrity
  - presence of declared indexes
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import duckdb
import pytest

from source.models import TABLES_IN_ORDER, create_source_tables


def _t(year: int = 2026, month: int = 6, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


@pytest.fixture()
def src() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    # one valid customer, payment method and wallet so we can test
    # downstream constraint rejections without setup noise
    c.execute(
        "INSERT INTO customers VALUES "
        "('c1', 'Alice', 'a@x.test', 'US', 'active', ?, ?, ?)",
        [date(2025, 1, 1), _t(), _t()],
    )
    c.execute(
        "INSERT INTO wallets VALUES "
        "('w1', 'c1', 'USD', 100.00, 'active', ?, ?)",
        [_t(), _t()],
    )
    c.execute(
        "INSERT INTO wallets VALUES "
        "('w2', 'c1', 'USD', 0.00, 'active', ?, ?)",
        [_t(), _t()],
    )
    return c


# ── PK uniqueness ─────────────────────────────────────────────────────────────


def test_duplicate_customer_id_rejected(src):
    src.execute(
        "INSERT INTO customers VALUES "
        "('c2', 'Bob', 'b@x.test', 'GB', 'active', ?, ?, ?)",
        [date(2025, 1, 1), _t(), _t()],
    )
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO customers VALUES "
            "('c2', 'Bob2', 'b2@x.test', 'GB', 'active', ?, ?, ?)",
            [date(2025, 1, 1), _t(), _t()],
        )


def test_duplicate_wallet_id_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO wallets VALUES "
            "('w1', 'c1', 'USD', 0.00, 'active', ?, ?)",
            [_t(), _t()],
        )


# ── NOT NULL ──────────────────────────────────────────────────────────────────


def test_null_customer_email_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO customers VALUES "
            "('c3', 'Chen', NULL, 'US', 'active', ?, ?, ?)",
            [date(2025, 1, 1), _t(), _t()],
        )


def test_null_wallet_currency_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO wallets VALUES "
            "('w_bad', 'c1', NULL, 0.00, 'active', ?, ?)",
            [_t(), _t()],
        )


# ── CHECK constraints ────────────────────────────────────────────────────────


def test_negative_wallet_balance_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO wallets VALUES "
            "('w_neg', 'c1', 'USD', -1.00, 'active', ?, ?)",
            [_t(), _t()],
        )


def test_invalid_customer_status_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO customers VALUES "
            "('c_bad', 'X', 'x@x.test', 'US', 'invalid', ?, ?, ?)",
            [date(2025, 1, 1), _t(), _t()],
        )


def test_zero_transfer_amount_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO transfers VALUES ("
            "'tx_bad', 'w1', 'w2', NULL, 0.00, 'USD', 'pending', "
            "NULL, NULL, ?, NULL)",
            [_t()],
        )


def test_self_transfer_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO transfers VALUES ("
            "'tx_self', 'w1', 'w1', NULL, 1.00, 'USD', 'pending', "
            "NULL, NULL, ?, NULL)",
            [_t()],
        )


def test_settled_at_before_created_at_rejected(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO transfers VALUES ("
            "'tx_back', 'w1', 'w2', NULL, 1.00, 'USD', 'settled', "
            "NULL, NULL, ?, ?)",
            [_t(2026, 6, 2), _t(2026, 6, 1)],
        )


def test_signed_amount_inconsistent_rejected(src):
    # credit but signed_amount = -amount
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO ledger_entries VALUES ("
            "'le_bad', 'w1', NULL, 'credit', 10.00, -10.00, 0.00, ?)",
            [_t()],
        )


# ── FK referential integrity ─────────────────────────────────────────────────


def test_wallet_references_existing_customer(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO wallets VALUES "
            "('w_orph', 'c_missing', 'USD', 0.00, 'active', ?, ?)",
            [_t(), _t()],
        )


def test_transfer_references_existing_wallets(src):
    with pytest.raises(duckdb.ConstraintException):
        src.execute(
            "INSERT INTO transfers VALUES ("
            "'tx_orph', 'w_missing', 'w2', NULL, 1.00, 'USD', 'pending', "
            "NULL, NULL, ?, NULL)",
            [_t()],
        )


# ── indexes are present ──────────────────────────────────────────────────────


@pytest.mark.parametrize("table", TABLES_IN_ORDER)
def test_table_exists(src: duckdb.DuckDBPyConnection, table: str):
    rows = src.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchall()
    assert rows, f"table {table} not created"


def test_indexes_declared_for_cdc_workload(src: duckdb.DuckDBPyConnection):
    indexes = [
        r[0]
        for r in src.execute(
            "SELECT index_name FROM duckdb_indexes()"
        ).fetchall()
    ]
    must_have = [
        "ix_customers_updated_at",
        "ix_wallets_customer",
        "ix_transfers_status",
        "ix_ledger_wallet_time",
    ]
    for ix in must_have:
        assert ix in indexes, f"missing required index {ix}"
