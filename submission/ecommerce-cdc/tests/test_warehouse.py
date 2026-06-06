"""Warehouse tests: latest snapshot, soft delete, SCD2 time travel, restore."""

from ingestion.pipeline import run_ingestion
from warehouse.warehouse import reconstruct_as_of, restore_from_lake


def test_latest_snapshot_reflects_updates(ingested):
    status, total = ingested.execute(
        "SELECT status, total FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()
    assert status == "paid"  # update applied
    assert float(total) == 70.00


def test_delete_soft_deletes_current(ingested):
    deleted = ingested.execute(
        "SELECT _deleted FROM wh_customers WHERE customer_id = 'c3'"
    ).fetchone()[0]
    assert deleted is True

    visible = ingested.execute(
        "SELECT COUNT(*) FROM wh_customers WHERE NOT _deleted"
    ).fetchone()[0]
    assert visible == 2  # c1, c2


def test_time_travel_order_status(ingested):
    # At seq 9 the order was still 'pending' (update happened at seq 10).
    early = reconstruct_as_of(ingested, "orders", as_of_seq=9)
    assert early[0]["status"] == "pending"

    # At seq 12 the order is 'paid'.
    late = reconstruct_as_of(ingested, "orders", as_of_seq=12)
    assert late[0]["status"] == "paid"


def test_time_travel_shows_deleted_row_before_delete(ingested):
    # c3 deleted at seq 12; at seq 11 it still existed.
    before = {r["customer_id"] for r in reconstruct_as_of(ingested, "customers", 11)}
    after = {r["customer_id"] for r in reconstruct_as_of(ingested, "customers", 12)}
    assert "c3" in before
    assert "c3" not in after


def test_restore_from_lake_rebuilds_snapshot(ingested):
    restore_from_lake(ingested, target_seq=9)
    status = ingested.execute(
        "SELECT status FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert status == "pending"

    # c3 not yet deleted at seq 9 -> present and not soft-deleted.
    c3 = ingested.execute(
        "SELECT _deleted FROM wh_customers WHERE customer_id = 'c3'"
    ).fetchone()
    assert c3 is not None and c3[0] is False


def test_scd2_history_has_two_order_versions(ingested):
    versions = ingested.execute(
        "SELECT COUNT(*) FROM wh_orders_history WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert versions == 2  # pending version + paid version

    current = ingested.execute(
        "SELECT status FROM wh_orders_history " "WHERE order_id = 'o1' AND _is_current"
    ).fetchone()[0]
    assert current == "paid"


def test_out_of_order_duplicate_apply_is_safe(conn, capture):
    run_ingestion(conn, capture)
    # Re-applying the same records must not change the snapshot.
    from warehouse.warehouse import apply_cdc_records

    apply_cdc_records(conn, capture.records_since(0))
    status = conn.execute(
        "SELECT status FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert status == "paid"
    versions = conn.execute(
        "SELECT COUNT(*) FROM wh_orders_history WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert versions == 2
