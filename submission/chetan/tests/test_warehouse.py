"""
Tests for Warehouse current-state snapshot upserts, soft-deletes, and time travel recovery.
"""

from datetime import datetime, timezone

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake
from pipeline.warehouse import apply_cdc_records, reconstruct_snapshot_at


def test_warehouse_upsert_and_soft_delete(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify CDC records upsert current snapshot and mark deletes soft-deleted."""
    now_str = datetime.now(timezone.utc).isoformat()
    cdc = CDCCapture()

    # 1. Insert customer
    cdc.insert(
        "customers",
        "c50",
        {
            "customer_id": "c50",
            "name": "David",
            "email": "d@d.com",
            "status": "active",
            "created_at": now_str,
            "updated_at": now_str,
        },
    )
    apply_cdc_records(conn, cdc.log)

    row = conn.execute(
        "SELECT name, status, _deleted, _cdc_seq FROM wh_customers WHERE customer_id = 'c50'"
    ).fetchone()
    assert row == ("David", "active", False, 1)

    # 2. Update customer status to suspended
    cdc.update(
        "customers",
        "c50",
        {
            "customer_id": "c50",
            "name": "David",
            "email": "d@d.com",
            "status": "suspended",
            "created_at": now_str,
            "updated_at": now_str,
        },
    )
    apply_cdc_records(conn, cdc.log)

    row_upd = conn.execute(
        "SELECT status, _deleted, _cdc_seq FROM wh_customers WHERE customer_id = 'c50'"
    ).fetchone()
    assert row_upd == ("suspended", False, 2)

    # 3. Delete customer
    cdc.delete(
        "customers",
        "c50",
        {"customer_id": "c50"},
    )
    apply_cdc_records(conn, cdc.log)

    row_del = conn.execute(
        "SELECT status, _deleted, _cdc_seq FROM wh_customers WHERE customer_id = 'c50'"
    ).fetchone()
    assert row_del == ("suspended", True, 3)


def test_warehouse_time_travel_reconstruction(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify historical reconstruction of warehouse state at prior CDC sequence numbers."""
    now_str = datetime.now(timezone.utc).isoformat()
    cdc = CDCCapture()

    # Seq 1: Insert customer c10 (Alice)
    cdc.insert(
        "customers",
        "c10",
        {
            "customer_id": "c10",
            "name": "Alice",
            "email": "a@a.com",
            "status": "active",
            "created_at": now_str,
            "updated_at": now_str,
        },
    )
    # Seq 2: Update customer c10 (Alice -> Alice Vance)
    cdc.update(
        "customers",
        "c10",
        {
            "customer_id": "c10",
            "name": "Alice Vance",
            "email": "a@a.com",
            "status": "active",
            "created_at": now_str,
            "updated_at": now_str,
        },
    )
    # Seq 3: Insert customer c20 (Bob)
    cdc.insert(
        "customers",
        "c20",
        {
            "customer_id": "c20",
            "name": "Bob",
            "email": "b@b.com",
            "status": "active",
            "created_at": now_str,
            "updated_at": now_str,
        },
    )
    # Seq 4: Delete customer c10
    cdc.delete("customers", "c10", {"customer_id": "c10"})

    # Write all events to Lake
    append_to_lake(conn, cdc.log)

    # Reconstruct at Seq 1: Expect Alice only
    state_seq1 = reconstruct_snapshot_at(conn, target_seq=1)
    assert len(state_seq1["customers"]) == 1
    assert state_seq1["customers"][0]["name"] == "Alice"

    # Reconstruct at Seq 2: Expect Alice Vance
    state_seq2 = reconstruct_snapshot_at(conn, target_seq=2)
    assert len(state_seq2["customers"]) == 1
    assert state_seq2["customers"][0]["name"] == "Alice Vance"

    # Reconstruct at Seq 3: Expect Alice Vance AND Bob
    state_seq3 = reconstruct_snapshot_at(conn, target_seq=3)
    assert len(state_seq3["customers"]) == 2

    # Reconstruct at Seq 4: Expect Bob only (Alice Vance was deleted at seq 4)
    state_seq4 = reconstruct_snapshot_at(conn, target_seq=4)
    assert len(state_seq4["customers"]) == 1
    assert state_seq4["customers"][0]["name"] == "Bob"
