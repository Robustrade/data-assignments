from datetime import timedelta

import pytest

from pipeline import warehouse
from scripts.run_data_quality_checks import run_all, enforce, DataQualityViolation

from tests.helpers import customer_row, order_row, order_item_row, payment_row


def _apply(warehouse_conn, change_log):
    warehouse.apply_lake_events(warehouse_conn, change_log.events)


def test_clean_data_has_no_violations(warehouse_conn, change_log):
    change_log.insert("customers", "c1", customer_row("c1"))
    change_log.insert("orders", "o1", order_row("o1", "c1", total_amount="20.00"))
    change_log.insert("order_items", "i1", order_item_row("i1", "o1", quantity=2, unit_price="10.00"))
    _apply(warehouse_conn, change_log)

    assert run_all(warehouse_conn, change_log.events) == {}


def test_null_required_field_is_caught(warehouse_conn, change_log):
    change_log.insert("customers", "c1", customer_row("c1", name=None))
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "not_null" in failures


def test_orphaned_foreign_key_is_caught(warehouse_conn, change_log):
    change_log.insert("order_items", "i1", order_item_row("i1", order_id="no-such-order"))
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "referential_integrity" in failures


def test_unexpected_enum_value_is_caught(warehouse_conn, change_log):
    change_log.insert("orders", "o1", order_row("o1"))
    change_log.insert("payments", "p1", payment_row("p1", "o1", status="not_a_real_status"))
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "enum_domain" in failures


def test_order_total_mismatch_with_items_is_caught(warehouse_conn, change_log):
    change_log.insert("orders", "o1", order_row("o1", total_amount="999.00"))
    change_log.insert("order_items", "i1", order_item_row("i1", "o1", quantity=1, unit_price="10.00"))
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "order_totals_match_items" in failures


def test_payment_before_order_creation_is_caught(warehouse_conn, change_log):
    row = payment_row("p1", "o1")
    row["paid_at"] = row["created_at"] - timedelta(days=1)
    change_log.insert("orders", "o1", order_row("o1"))
    change_log.insert("payments", "p1", row)
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "payment_timing" in failures


def test_illegal_status_transition_is_caught(warehouse_conn, change_log):
    change_log.insert("orders", "o1", order_row("o1", status="pending"))
    change_log.update("orders", "o1", order_row("o1", status="cancelled"))
    change_log.update("orders", "o1", order_row("o1", status="paid"))  # cancelled -> paid is illegal
    _apply(warehouse_conn, change_log)

    failures = run_all(warehouse_conn, change_log.events)
    assert "order_status_transitions" in failures


def test_enforce_raises_when_anything_fails(warehouse_conn, change_log):
    change_log.insert("customers", "c1", customer_row("c1", name=None))
    _apply(warehouse_conn, change_log)

    with pytest.raises(DataQualityViolation):
        enforce(warehouse_conn, change_log.events)
