"""
Tests — data quality, validation rules, and warehouse correctness.
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pytest

from pipeline.lake import append_to_lake
from pipeline.warehouse import apply_cdc_records
from scripts.run_data_quality_checks import run_checks


def _ts() -> datetime:
    return datetime.now(timezone.utc)


def test_insert_appears_in_warehouse(seeded):
    conn, _ = seeded
    row = conn.execute(
        "SELECT company_name FROM wh_trade_partners WHERE partner_id = 'p1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Indo Exporters Ltd"


def test_insert_appears_in_lake(seeded):
    conn, _ = seeded
    count = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events WHERE table_name = 'trade_partners' AND operation = 'insert'"
    ).fetchone()[0]
    assert count == 2


def test_update_overwrites_warehouse_row(seeded):
    conn, capture = seeded
    ts = _ts()
    capture.update(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Indo Exporters Updated",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "suspended",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    new_records = capture.records_since(capture.latest_sequence - 1)
    apply_cdc_records(conn, new_records)

    row = conn.execute(
        "SELECT company_name, compliance_status FROM wh_trade_partners WHERE partner_id = 'p1'"
    ).fetchone()
    assert row[0] == "Indo Exporters Updated"
    assert row[1] == "suspended"


def test_delete_marks_warehouse_row_as_deleted(seeded):
    conn, capture = seeded
    capture.delete("trade_partners", "p2", {"partner_id": "p2"})
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    row = conn.execute(
        "SELECT _deleted FROM wh_trade_partners WHERE partner_id = 'p2'"
    ).fetchone()
    assert row is not None
    assert row[0] is True


def test_lake_row_count_only_increases(seeded):
    conn, capture = seeded
    before = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]

    ts = _ts()
    capture.update(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Indo Exporters Updated Again",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    append_to_lake(conn, capture.records_since(capture.latest_sequence - 1))
    after = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert after > before


def test_business_math_reconciliation_passes_on_valid_data(seeded):
    conn, _ = seeded
    failures = run_checks(conn)
    # Ensure no calculation errors or overpayment triggers
    assert not any("total_value mismatch" in f for f in failures)
    assert not any("is overpaid" in f for f in failures)


def test_business_validation_fails_on_overpayment(seeded):
    conn, capture = seeded
    # The active contract has value 125,000.00. tx1 paid 50,000.00.
    # Let's add a transaction of 100,000.00, bringing total payments to 150,000.00 (which is > 125,000.00).
    ts = _ts()
    capture.insert(
        "settlement_transactions",
        "tx2",
        {
            "transaction_id": "tx2",
            "contract_id": "con1",
            "payment_amount_usd": Decimal("100000.00"),
            "settlement_type": "wire_transfer",
            "status": "settled",
            "settled_at": ts,
        },
    )
    new_records = capture.records_since(capture.latest_sequence - 1)
    apply_cdc_records(conn, new_records)

    failures = run_checks(conn)
    assert any("is overpaid" in f for f in failures)


def test_customs_clearance_status_blocks_delivery(seeded):
    conn, capture = seeded
    ts = _ts()
    # Update customs status to 'rejected'
    capture.update(
        "customs_clearances",
        "cl1",
        {
            "clearance_id": "cl1",
            "contract_id": "con1",
            "clearing_country": "India",
            "duty_fee_usd": Decimal("2500.00"),
            "status": "rejected",
            "cleared_at": None,
        },
    )
    # Update cargo status to delivered (actual_delivery set to timestamp)
    capture.update(
        "shipment_cargo",
        "car1",
        {
            "cargo_id": "car1",
            "contract_id": "con1",
            "carrier_name": "MSC Logistics",
            "container_number": "MSCU1234567",
            "port_of_loading": "Jakarta",
            "port_of_discharge": "Nhava Sheva",
            "estimated_delivery": ts + timedelta(days=10),
            "actual_delivery": ts,
        },
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 2))

    failures = run_checks(conn)
    assert any("cargo delivery/deliveries completed without approved customs clearance" in f for f in failures)
