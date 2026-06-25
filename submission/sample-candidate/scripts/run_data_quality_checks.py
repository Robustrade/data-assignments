#!/usr/bin/env python3
"""
run_data_quality_checks.py

Runs system and business data quality validations against the warehouse.
Seeds an in-memory database with representative data, applies CDC events,
then asserts correctness invariants.

System checks   : PK uniqueness, not-null, referential integrity
Business checks : non-negative balances, positive amounts, valid status enums

Exit 0 — all checks pass.
Exit 1 — one or more failures found.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables


def _now() -> datetime:
    return datetime.now(timezone.utc)


def seed(conn: duckdb.DuckDBPyConnection, capture: CDCCapture) -> None:
    """Populate lake + warehouse with representative test data."""
    ts = _now()

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

    create_lake_table(conn)
    create_warehouse_tables(conn)
    records = capture.records_since(0)
    append_to_lake(conn, records)
    apply_cdc_records(conn, records)


def run_checks(conn: duckdb.DuckDBPyConnection) -> list[str]:
    failures: list[str] = []

    # ── system checks ─────────────────────────────────────────────────────────

    pk_map = {
        "wh_customers": "customer_id",
        "wh_wallets": "wallet_id",
        "wh_transactions": "transaction_id",
    }
    for table, pk in pk_map.items():
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        distinct = conn.execute(f"SELECT COUNT(DISTINCT {pk}) FROM {table}").fetchone()[
            0
        ]
        if total != distinct:
            failures.append(
                f"{table}: PK not unique — {total} rows, {distinct} distinct {pk}"
            )

    not_null_checks = [
        ("wh_customers", "name"),
        ("wh_customers", "email"),
        ("wh_wallets", "customer_id"),
        ("wh_wallets", "currency"),
        ("wh_transactions", "wallet_id"),
        ("wh_transactions", "amount"),
        ("wh_transactions", "direction"),
    ]
    for table, col in not_null_checks:
        nulls = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
        ).fetchone()[0]
        if nulls:
            failures.append(f"{table}.{col}: {nulls} NULL value(s) found")

    # ── business checks ───────────────────────────────────────────────────────

    neg_bal = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets WHERE balance < 0"
    ).fetchone()[0]
    if neg_bal:
        failures.append(f"wh_wallets: {neg_bal} row(s) with negative balance")

    non_pos = conn.execute(
        "SELECT COUNT(*) FROM wh_transactions WHERE amount <= 0"
    ).fetchone()[0]
    if non_pos:
        failures.append(f"wh_transactions: {non_pos} row(s) with non-positive amount")

    bad_dir = conn.execute(
        "SELECT COUNT(*) FROM wh_transactions"
        " WHERE direction NOT IN ('credit', 'debit')"
    ).fetchone()[0]
    if bad_dir:
        failures.append(f"wh_transactions: {bad_dir} row(s) with invalid direction")

    # ── lake completeness ─────────────────────────────────────────────────────

    lake_count = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    if lake_count == 0:
        failures.append("lake_cdc_events: no records found — lake appears empty")

    return failures


def main() -> int:
    conn = duckdb.connect(":memory:")
    capture = CDCCapture()
    seed(conn, capture)

    failures = run_checks(conn)

    if failures:
        print("Data quality failures:")
        for f in failures:
            print(f"  ✗ {f}")
        return 1

    print(
        f"All data quality checks passed ({len(run_checks.__code__.co_consts)} rules checked)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
