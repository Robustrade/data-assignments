#!/usr/bin/env python3
"""
run_data_quality_checks.py

Executes system and business data quality checks against source tables and downstream
warehouse snapshots to ensure validation parity.

Exit 0 — all data quality checks passed.
Exit 1 — one or more data quality violations detected.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

# Ensure workspace root and submission paths are available
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.validations import run_validation_suite
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables
from source.models import create_source_tables


def populate_demo_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Seed source, lake, and warehouse with valid demo transactional data."""
    create_source_tables(conn)
    create_lake_table(conn)
    create_warehouse_tables(conn)

    now = datetime.now(timezone.utc)

    # 1. Insert Customers
    conn.execute(
        "INSERT INTO customers VALUES ('cust_101', 'Alice Johnson', 'alice@example.com', 'active', ?, ?)",
        [now, now],
    )
    conn.execute(
        "INSERT INTO customers VALUES ('cust_102', 'Bob Smith', 'bob@example.com', 'active', ?, ?)",
        [now, now],
    )

    # 2. Insert Wallets
    conn.execute(
        "INSERT INTO wallets VALUES ('wal_101', 'cust_101', 500.00, 'USD', 'active', ?, ?)",
        [now, now],
    )
    conn.execute(
        "INSERT INTO wallets VALUES ('wal_102', 'cust_102', 1250.50, 'USD', 'active', ?, ?)",
        [now, now],
    )

    # 3. Insert Merchants
    conn.execute(
        "INSERT INTO merchants VALUES ('merch_1', 'Acme Store', 'support@acme.com', 'active', ?)",
        [now],
    )

    # 4. Insert Transactions
    conn.execute(
        "INSERT INTO transactions VALUES ('tx_1001', 'wal_101', 'merch_1', 45.00, 1.50, 'debit', 'settled', 'REF1001', ?, ?)",
        [now, now],
    )

    # 5. Insert Ledger Entries
    conn.execute(
        "INSERT INTO ledger_entries VALUES ('leg_1', 'tx_1001', 'wal_101', 'debit', 45.00, 455.00, ?)",
        [now],
    )

    # Simulate CDC capture and stream processing to Lake and Warehouse
    cdc = CDCCapture()
    cdc.insert(
        "customers",
        "cust_101",
        {
            "customer_id": "cust_101",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    )
    cdc.insert(
        "wallets",
        "wal_101",
        {
            "wallet_id": "wal_101",
            "customer_id": "cust_101",
            "balance": 500.00,
            "currency": "USD",
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    )
    cdc.insert(
        "merchants",
        "merch_1",
        {
            "merchant_id": "merch_1",
            "name": "Acme Store",
            "email": "support@acme.com",
            "status": "active",
            "created_at": now.isoformat(),
        },
    )
    cdc.insert(
        "transactions",
        "tx_1001",
        {
            "transaction_id": "tx_1001",
            "wallet_id": "wal_101",
            "merchant_id": "merch_1",
            "amount": 45.00,
            "fee": 1.50,
            "direction": "debit",
            "status": "settled",
            "reference": "REF1001",
            "created_at": now.isoformat(),
            "settled_at": now.isoformat(),
        },
    )
    cdc.insert(
        "ledger_entries",
        "leg_1",
        {
            "entry_id": "leg_1",
            "transaction_id": "tx_1001",
            "wallet_id": "wal_101",
            "entry_type": "debit",
            "amount": 45.00,
            "balance_after": 455.00,
            "created_at": now.isoformat(),
        },
    )

    recs = cdc.log
    append_to_lake(conn, recs)
    apply_cdc_records(conn, recs)


def main() -> int:
    conn = duckdb.connect(":memory:")
    populate_demo_data(conn)

    violations = run_validation_suite(conn)

    if violations:
        print("Data quality validation checks FAILED:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print("All system and business data quality checks PASSED successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
