#!/usr/bin/env python3
"""
run_data_quality_checks.py

Runs system and business data quality validations against the warehouse.
Seeds an in-memory database with representative data, applies CDC events,
then asserts correctness invariants.

System checks   : PK uniqueness, not-null, referential integrity
Business checks : positive weight/pricing, valid enum values, contract total reconciliation, customs delivery blocks

Exit 0 — all checks pass.
Exit 1 — one or more failures found.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import duckdb

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables


def _now() -> datetime:
    return datetime.now(timezone.utc)


def seed(conn: duckdb.DuckDBPyConnection, capture: CDCCapture) -> None:
    """Populate lake + warehouse with representative test data."""
    ts = _now()

    # 1. Seed partners
    capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Indo Exporters Ltd",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "trade_partners",
        "p2",
        {
            "partner_id": "p2",
            "company_name": "Robustrade India",
            "country": "India",
            "partner_type": "buyer",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    # 2. Seed contracts
    capture.insert(
        "trade_contracts",
        "con1",
        {
            "contract_id": "con1",
            "seller_id": "p1",
            "buyer_id": "p2",
            "commodity_type": "Coffee Beans",
            "weight_metric_tons": Decimal("20.0000"),
            "price_per_ton_usd": Decimal("3000.00"),
            "total_value_usd": Decimal("60000.00"),
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    # 3. Seed cargo
    capture.insert(
        "shipment_cargo",
        "car1",
        {
            "cargo_id": "car1",
            "contract_id": "con1",
            "carrier_name": "Maersk Line",
            "container_number": "MSKU9938210",
            "port_of_loading": "Jakarta",
            "port_of_discharge": "Nhava Sheva",
            "estimated_delivery": ts + timedelta(days=14),
            "actual_delivery": None,
        },
    )

    # 4. Seed customs
    capture.insert(
        "customs_clearances",
        "cl1",
        {
            "clearance_id": "cl1",
            "contract_id": "con1",
            "clearing_country": "India",
            "duty_fee_usd": Decimal("1200.00"),
            "status": "pending",
            "cleared_at": None,
        },
    )

    # 5. Seed settlement transaction
    capture.insert(
        "settlement_transactions",
        "tx1",
        {
            "transaction_id": "tx1",
            "contract_id": "con1",
            "payment_amount_usd": Decimal("30000.00"),
            "settlement_type": "letter_of_credit",
            "status": "settled",
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

    # ── 1. SYSTEM QUALITY CHECKS ──────────────────────────────────────────────

    # Check PK uniqueness
    pk_map = {
        "wh_trade_partners": "partner_id",
        "wh_trade_contracts": "contract_id",
        "wh_shipment_cargo": "cargo_id",
        "wh_customs_clearances": "clearance_id",
        "wh_settlement_transactions": "transaction_id",
    }
    for table, pk in pk_map.items():
        total = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE _deleted = false").fetchone()[0]
        distinct = conn.execute(f"SELECT COUNT(DISTINCT {pk}) FROM {table} WHERE _deleted = false").fetchone()[0]
        if total != distinct:
            failures.append(
                f"{table}: PK not unique — {total} active rows, {distinct} distinct {pk}"
            )

    # Check NOT NULL constraints
    not_null_checks = [
        ("wh_trade_partners", "company_name"),
        ("wh_trade_partners", "country"),
        ("wh_trade_partners", "partner_type"),
        ("wh_trade_contracts", "seller_id"),
        ("wh_trade_contracts", "buyer_id"),
        ("wh_trade_contracts", "commodity_type"),
        ("wh_trade_contracts", "total_value_usd"),
        ("wh_shipment_cargo", "contract_id"),
        ("wh_shipment_cargo", "container_number"),
        ("wh_customs_clearances", "contract_id"),
        ("wh_customs_clearances", "duty_fee_usd"),
        ("wh_settlement_transactions", "contract_id"),
        ("wh_settlement_transactions", "payment_amount_usd"),
    ]
    for table, col in not_null_checks:
        nulls = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL AND _deleted = false"
        ).fetchone()[0]
        if nulls:
            failures.append(f"{table}.{col}: {nulls} NULL value(s) found in active rows")

    # Check Referential Integrity in Warehouse
    # Every contract must link to an existing partner
    dangling_contracts = conn.execute(
        """
        SELECT COUNT(*) FROM wh_trade_contracts c
        LEFT JOIN wh_trade_partners p1 ON c.seller_id = p1.partner_id
        LEFT JOIN wh_trade_partners p2 ON c.buyer_id = p2.partner_id
        WHERE (p1.partner_id IS NULL OR p2.partner_id IS NULL) AND c._deleted = false
        """
    ).fetchone()[0]
    if dangling_contracts:
        failures.append(f"wh_trade_contracts: {dangling_contracts} row(s) referencing missing trade partners")

    # Every cargo/clearance/transaction must link to an existing contract
    child_tables = ["wh_shipment_cargo", "wh_customs_clearances", "wh_settlement_transactions"]
    for t in child_tables:
        dangling = conn.execute(
            f"""
            SELECT COUNT(*) FROM {t} child
            LEFT JOIN wh_trade_contracts parent ON child.contract_id = parent.contract_id
            WHERE parent.contract_id IS NULL AND child._deleted = false
            """
        ).fetchone()[0]
        if dangling:
            failures.append(f"{t}: {dangling} row(s) referencing missing trade contracts")

    # ── 2. BUSINESS RULE CHECKS ───────────────────────────────────────────────

    # Invariant 1: Total Value = weight * price_per_ton
    bad_math = conn.execute(
        """
        SELECT COUNT(*) FROM wh_trade_contracts
        WHERE abs(total_value_usd - (weight_metric_tons * price_per_ton_usd)) > 0.01
          AND _deleted = false
        """
    ).fetchone()[0]
    if bad_math:
        failures.append(f"wh_trade_contracts: {bad_math} row(s) with total_value mismatch (math drift)")

    # Invariant 2: Total settled payments <= contract value
    overpaid = conn.execute(
        """
        SELECT c.contract_id, c.total_value_usd, sum(t.payment_amount_usd) as paid
        FROM wh_trade_contracts c
        JOIN wh_settlement_transactions t ON c.contract_id = t.contract_id
        WHERE c._deleted = false AND t._deleted = false AND t.status = 'settled'
        GROUP BY c.contract_id, c.total_value_usd
        HAVING sum(t.payment_amount_usd) > c.total_value_usd
        """
    ).fetchall()
    if overpaid:
        for row in overpaid:
            failures.append(
                f"wh_settlement_transactions: contract {row[0]} is overpaid. Contract value: {row[1]}, Settled: {row[2]}"
            )

    # Invariant 3: Cargo cannot be delivered (actual_delivery IS NOT NULL) if customs clearance status is rejected or held
    blocked_delivery = conn.execute(
        """
        SELECT COUNT(*) FROM wh_shipment_cargo s
        JOIN wh_customs_clearances c ON s.contract_id = c.contract_id
        WHERE s._deleted = false AND c._deleted = false
          AND s.actual_delivery IS NOT NULL
          AND c.status IN ('rejected', 'held_in_customs')
        """
    ).fetchone()[0]
    if blocked_delivery:
        failures.append(
            f"wh_shipment_cargo: {blocked_delivery} cargo delivery/deliveries completed without approved customs clearance"
        )

    # Invariant 4: Positivity and Domain checks
    neg_weights = conn.execute(
        "SELECT COUNT(*) FROM wh_trade_contracts WHERE (weight_metric_tons <= 0 OR price_per_ton_usd <= 0) AND _deleted = false"
    ).fetchone()[0]
    if neg_weights:
        failures.append(f"wh_trade_contracts: {neg_weights} contract(s) with non-positive weight or pricing")

    neg_duties = conn.execute(
        "SELECT COUNT(*) FROM wh_customs_clearances WHERE duty_fee_usd < 0 AND _deleted = false"
    ).fetchone()[0]
    if neg_duties:
        failures.append(f"wh_customs_clearances: {neg_duties} clearance(s) with negative duty fee")

    # Invariant 5: Status domain checks
    bad_contract_status = conn.execute(
        "SELECT COUNT(*) FROM wh_trade_contracts WHERE status NOT IN ('pending', 'active', 'completed', 'cancelled') AND _deleted = false"
    ).fetchone()[0]
    if bad_contract_status:
        failures.append(f"wh_trade_contracts: {bad_contract_status} contract(s) with invalid status enum")

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

    print("All data quality checks passed (system validations and business rules verified).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
