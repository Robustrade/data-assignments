"""Shared pytest fixtures for the payments CDC pipeline tests."""

from datetime import datetime, timezone

import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables
from source.models import create_source_tables


def _ts() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def conn() -> duckdb.DuckDBPyConnection:
    """Fresh in-memory DuckDB with source + lake + warehouse tables."""
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    create_lake_table(c)
    create_warehouse_tables(c)
    return c


@pytest.fixture()
def capture() -> CDCCapture:
    """Empty CDC capture log."""
    return CDCCapture()


@pytest.fixture()
def seeded(conn: duckdb.DuckDBPyConnection, capture: CDCCapture):
    """
    DuckDB connection pre-loaded with two customers, two wallets,
    and two transactions — all flushed through lake and warehouse.
    Returns (conn, capture).
    """
    ts = _ts()

    capture.insert(
        "customers",
        "c1",
        {
            "customer_id": "c1",
            "name": "Alice",
            "email": "alice-test-user",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "customers",
        "c2",
        {
            "customer_id": "c2",
            "name": "Bob",
            "email": "bob-test-user",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "wallets",
        "w1",
        {
            "wallet_id": "w1",
            "customer_id": "c1",
            "balance": 100.00,
            "currency": "USD",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "wallets",
        "w2",
        {
            "wallet_id": "w2",
            "customer_id": "c2",
            "balance": 50.00,
            "currency": "USD",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "transactions",
        "t1",
        {
            "transaction_id": "t1",
            "wallet_id": "w1",
            "amount": 25.00,
            "direction": "credit",
            "status": "settled",
            "reference": "ref-001",
            "created_at": ts,
            "settled_at": ts,
        },
    )
    capture.insert(
        "transactions",
        "t2",
        {
            "transaction_id": "t2",
            "wallet_id": "w2",
            "amount": 10.00,
            "direction": "debit",
            "status": "settled",
            "reference": "ref-002",
            "created_at": ts,
            "settled_at": ts,
        },
    )

    records = capture.records_since(0)
    append_to_lake(conn, records)
    apply_cdc_records(conn, records)
    return conn, capture
