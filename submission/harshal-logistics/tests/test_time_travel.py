"""
Tests — Point-in-Time Recovery and Time-Travel verification.
"""

from datetime import datetime, timezone
from decimal import Decimal
import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake
from pipeline.warehouse import apply_cdc_records, reconstruct_warehouse_at


def _ts() -> datetime:
    return datetime.now(timezone.utc)


def test_time_travel_reconstructs_exact_history(conn, capture):
    ts = _ts()

    # Step 1: Add a partner (sequence 1)
    capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Jakarta Cargo Inc",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    # Step 2: Add contract (sequence 2)
    capture.insert(
        "trade_contracts",
        "con1",
        {
            "contract_id": "con1",
            "seller_id": "p1",
            "buyer_id": "p2",  # (references non-existent buyer for now, checked in DQ layer)
            "commodity_type": "Copper Ore",
            "weight_metric_tons": Decimal("100.0000"),
            "price_per_ton_usd": Decimal("8000.00"),
            "total_value_usd": Decimal("800000.00"),
            "status": "pending",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    lsn_after_create = capture.latest_sequence  # Should be 2

    # Step 3: Update contract to status = 'active' (sequence 3)
    capture.update(
        "trade_contracts",
        "con1",
        {
            "contract_id": "con1",
            "seller_id": "p1",
            "buyer_id": "p2",
            "commodity_type": "Copper Ore",
            "weight_metric_tons": Decimal("100.0000"),
            "price_per_ton_usd": Decimal("8000.00"),
            "total_value_usd": Decimal("800000.00"),
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    lsn_after_active = capture.latest_sequence  # Should be 3

    # Step 4: Delete the contract (sequence 4)
    capture.delete("trade_contracts", "con1", {"contract_id": "con1"})
    lsn_after_delete = capture.latest_sequence  # Should be 4

    # Flush all simulated logs to Lake
    append_to_lake(conn, capture.records_since(0))

    # Apply all to Warehouse current state
    apply_cdc_records(conn, capture.records_since(0))

    # --- VERIFY CURRENT STATE ---
    # Current state should reflect the latest state (which is deleted)
    curr_status, curr_deleted = conn.execute(
        "SELECT status, _deleted FROM wh_trade_contracts WHERE contract_id = 'con1'"
    ).fetchone()
    assert curr_deleted is True

    # --- TIME TRAVEL 1: Reconstruct at lsn_after_create (LSN 2) ---
    reconstruct_warehouse_at(conn, lsn_after_create)
    status_at_create, deleted_at_create = conn.execute(
        "SELECT status, _deleted FROM wh_trade_contracts WHERE contract_id = 'con1'"
    ).fetchone()
    assert deleted_at_create is False
    assert status_at_create == "pending"

    # --- TIME TRAVEL 2: Reconstruct at lsn_after_active (LSN 3) ---
    reconstruct_warehouse_at(conn, lsn_after_active)
    status_at_active, deleted_at_active = conn.execute(
        "SELECT status, _deleted FROM wh_trade_contracts WHERE contract_id = 'con1'"
    ).fetchone()
    assert deleted_at_active is False
    assert status_at_active == "active"

    # --- TIME TRAVEL 3: Reconstruct at lsn_after_delete (LSN 4) ---
    reconstruct_warehouse_at(conn, lsn_after_delete)
    status_at_delete, deleted_at_delete = conn.execute(
        "SELECT status, _deleted FROM wh_trade_contracts WHERE contract_id = 'con1'"
    ).fetchone()
    assert deleted_at_delete is True


def test_deduplication_and_out_of_order_safety(conn, capture):
    ts = _ts()
    # Step 1: Insert partner (sequence 1)
    r1 = capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Initial Name",
            "country": "India",
            "partner_type": "buyer",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    # Step 2: Update partner name (sequence 2)
    r2 = capture.update(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Updated Name V2",
            "country": "India",
            "partner_type": "buyer",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    # Let's write them both to lake
    append_to_lake(conn, [r1, r2])

    # Apply r2 first, then r1 (out of order arrival simulation)
    apply_cdc_records(conn, [r2])
    # The name should be "Updated Name V2"
    name = conn.execute("SELECT company_name FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()[0]
    assert name == "Updated Name V2"

    # Now apply r1 (which has an older sequence = 1)
    apply_cdc_records(conn, [r1])
    # The name should STILL be "Updated Name V2" because r1 sequence (1) <= current _cdc_seq (2)
    name_after_old_arrival = conn.execute("SELECT company_name FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()[0]
    assert name_after_old_arrival == "Updated Name V2"

    # Apply r2 again (duplicate arrival simulation)
    apply_cdc_records(conn, [r2])
    # The name should STILL be "Updated Name V2" and no duplicates should be created
    count = conn.execute("SELECT COUNT(*) FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()[0]
    assert count == 1


def test_delete_before_insert_placeholder_handling(conn, capture):
    """
    If a DELETE event arrives out-of-order before any INSERT event, it creates
    a placeholder soft-deleted row. A subsequent INSERT (with higher LSN) updates it.
    """
    # 1. Delete arrives first (sequence 1)
    r1 = capture.delete("trade_partners", "p1", {"partner_id": "p1"})
    apply_cdc_records(conn, [r1])

    # Should have a soft-deleted placeholder row
    row = conn.execute(
        "SELECT _deleted, _cdc_seq, company_name FROM wh_trade_partners WHERE partner_id = 'p1'"
    ).fetchone()
    assert row is not None
    assert row[0] is True
    assert row[1] == 1
    assert row[2] is None  # Other fields are NULL

    # 2. Insert arrives late (sequence 2)
    ts = _ts()
    r2 = capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Late Arrival Co",
            "country": "Hong Kong",
            "partner_type": "buyer",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    apply_cdc_records(conn, [r2])

    # Placeholder should be fully populated and restored (not deleted)
    row = conn.execute(
        "SELECT _deleted, _cdc_seq, company_name, country FROM wh_trade_partners WHERE partner_id = 'p1'"
    ).fetchone()
    assert row[0] is False
    assert row[1] == 2
    assert row[2] == "Late Arrival Co"
    assert row[3] == "Hong Kong"


def test_resurrection_safety_older_insert_ignored(conn, capture):
    """
    If a row is soft-deleted at LSN 5, a late-arriving out-of-order INSERT
    at LSN 3 must be ignored, ensuring the row is not resurrected.
    """
    ts = _ts()
    # LSN 3: Insert
    r3 = capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Jakarta Cargo Inc",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    # LSN 5: Delete
    r5 = capture.delete("trade_partners", "p1", {"partner_id": "p1"})

    # Process delete (LSN 5) first
    apply_cdc_records(conn, [r5])

    # Row is soft-deleted
    row = conn.execute("SELECT _deleted, _cdc_seq FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()
    assert row[0] is True
    assert row[1] == 2  # Sequence is 2 since r5 delete is sequence 2 in capture session

    # Now let's manually override sequence to simulate real sequence values
    # capture generated r3 as sequence 1 and r5 as sequence 2.
    # Let's write a fresh sequence list to simulate: LSN 5 (delete) processed before LSN 3 (insert)
    r3.sequence = 3
    r5.sequence = 5

    # Reset tables
    conn.execute("DELETE FROM wh_trade_partners")
    
    # Process LSN 5 first
    apply_cdc_records(conn, [r5])
    row = conn.execute("SELECT _deleted, _cdc_seq FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()
    assert row[0] is True
    assert row[1] == 5

    # Process late-arriving LSN 3
    apply_cdc_records(conn, [r3])

    # Row must STILL be deleted and LSN remains 5 (not resurrected by older LSN 3)
    row = conn.execute("SELECT _deleted, _cdc_seq, company_name FROM wh_trade_partners WHERE partner_id = 'p1'").fetchone()
    assert row[0] is True
    assert row[1] == 5
    assert row[2] is None
