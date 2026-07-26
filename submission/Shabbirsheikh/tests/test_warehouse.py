from pipeline import warehouse

from tests.helpers import customer_row, order_row


def test_apply_lake_events_upserts_to_latest_state(warehouse_conn, change_log):
    change_log.insert("orders", "o1", order_row(status="pending"))
    change_log.update("orders", "o1", order_row(status="paid"))

    warehouse.apply_lake_events(warehouse_conn, change_log.events)

    row = warehouse_conn.execute(
        "SELECT status, _cdc_seq FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()
    assert row == ("paid", 2)


def test_stale_out_of_order_write_cannot_overwrite_a_newer_row(warehouse_conn, change_log):
    change_log.insert("orders", "o1", order_row(status="pending"))
    change_log.update("orders", "o1", order_row(status="paid"))
    warehouse.apply_lake_events(warehouse_conn, change_log.events)

    stale_event = dict(change_log.events[0])  # the original "pending" insert, sequence 1
    warehouse.apply_lake_events(warehouse_conn, [stale_event])  # already applied - must not undo the seq-2 "paid" write

    status = warehouse_conn.execute(
        "SELECT status FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert status == "paid"


def test_delete_soft_deletes_instead_of_removing_the_row(warehouse_conn, change_log):
    change_log.insert("customers", "c1", customer_row())
    change_log.delete("customers", "c1", customer_row())

    warehouse.apply_lake_events(warehouse_conn, change_log.events)

    row = warehouse_conn.execute(
        "SELECT customer_id, _deleted FROM wh_customers"
    ).fetchone()
    assert row == ("c1", True)


def test_reconstruct_as_of_shows_state_before_a_later_change(change_log):
    change_log.insert("orders", "o1", order_row(status="pending"))
    change_log.update("orders", "o1", order_row(status="paid"))

    bounded_events = [e for e in change_log.events if e["sequence"] <= 1]
    past_conn = warehouse.reconstruct_as_of(bounded_events)

    status = past_conn.execute(
        "SELECT status FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()[0]
    assert status == "pending"
