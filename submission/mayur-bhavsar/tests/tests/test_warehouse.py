"""Warehouse correctness: latest snapshot, tombstones, and derived marts."""

from __future__ import annotations

from decimal import Decimal

from cdclake import source as source_mod
from cdclake.config import TRACKED_TABLES
from cdclake.pipeline import reconcile


def test_current_state_reconciles_with_the_source(reference_platform):
    """The cheapest high-signal completeness check there is."""
    for table, counts in reconcile(reference_platform).items():
        assert counts["delta"] == 0, f"{table}: {counts}"


def test_every_tracked_entity_has_a_current_model(reference_platform):
    for table in TRACKED_TABLES:
        assert reference_platform.warehouse.current(table) is not None


def test_deleted_rows_are_absent_from_current_but_present_in_history(
    reference_platform,
):
    current = reference_platform.warehouse.current("customer_order")
    assert 1004 not in {row["order_id"] for row in current}

    history = reference_platform.warehouse.history("customer_order")
    versions = [row for row in history if row["order_id"] == 1004]
    assert versions, "the deleted order vanished from history — retention is broken"
    assert versions[-1]["is_deleted"] is True
    assert versions[-1]["is_current"] is True
    assert versions[-1]["_op"] == "D"


def test_a_tombstone_keeps_the_last_known_attribute_values(reference_platform):
    """A delete carries only a before image; the tombstone must still describe
    the row, otherwise the moment before the delete is unrecoverable."""
    history = reference_platform.warehouse.history("customer_order")
    tombstone = [r for r in history if r["order_id"] == 1004 and r["is_deleted"]][0]

    assert tombstone["status"] == "CANCELLED"
    assert tombstone["customer_id"] == 4
    assert tombstone["total_amount"] == Decimal("96.00")


def test_cascaded_child_deletes_are_reflected(reference_platform):
    items = reference_platform.warehouse.current("order_item")
    assert 1004 not in {row["order_id"] for row in items}


def test_current_state_matches_the_source_row_by_row(reference_platform):
    """Not just counts: the actual values."""
    expected = {
        row["order_id"]: row
        for row in source_mod.snapshot(reference_platform.conn, "customer_order")
    }
    actual = {
        row["order_id"]: row
        for row in reference_platform.warehouse.current("customer_order")
    }

    assert set(expected) == set(actual)
    for order_id, source_row in expected.items():
        warehouse_row = actual[order_id]
        assert warehouse_row["status"] == source_row["status"]
        assert warehouse_row["placed_at"] == source_row["placed_at"]
        assert warehouse_row["total_amount"] == Decimal(str(source_row["total_amount"]))


def test_a_price_change_does_not_rewrite_historical_line_items(reference_platform):
    """The classic dimensional trap. The workload raises product 1 from 129.00
    to 139.00 after order 1001 was placed; the line must keep the price that
    was actually charged."""
    product = [
        row
        for row in reference_platform.warehouse.current("product")
        if row["product_id"] == 1
    ][0]
    line = [
        row
        for row in reference_platform.warehouse.current("order_item")
        if row["order_id"] == 1001 and row["line_no"] == 1
    ][0]

    assert product["unit_price"] == Decimal("139.00")
    assert line["unit_price"] == Decimal("129.00")


def test_history_records_every_version_of_a_changed_row(reference_platform):
    history = [
        row
        for row in reference_platform.warehouse.history("customer_order")
        if row["order_id"] == 1001
    ]
    statuses = [row["status"] for row in history]

    assert statuses == ["PENDING", "PAID", "SHIPPED", "DELIVERED"]
    assert [row["is_current"] for row in history] == [False, False, False, True]
    assert all(row["valid_to"] is not None for row in history[:-1])
    assert history[-1]["valid_to"] is None


def test_intervals_chain_without_gaps(reference_platform):
    history = [
        row
        for row in reference_platform.warehouse.history("customer_order")
        if row["order_id"] == 1001
    ]
    for earlier, later in zip(history, history[1:], strict=False):
        assert earlier["valid_to"] == later["valid_from"]


def test_mart_totals_are_exact_decimals(reference_platform):
    rows = reference_platform.warehouse.query(
        "select order_id, total_amount, line_item_total, settled_amount, "
        "outstanding_amount from mart_order_financials order by order_id"
    )
    assert rows
    for row in rows:
        assert isinstance(row["total_amount"], Decimal)
        assert row["total_amount"] == row["line_item_total"]
        assert row["outstanding_amount"] == row["total_amount"] - row["settled_amount"]


def test_the_warehouse_never_reads_the_source(reference_platform):
    """Structural guarantee: models depend only on the lake.

    If any model referenced the source database the warehouse would stop being
    a pure function of the change log, and replay would no longer be safe.
    """
    relations = {
        row["table_name"]
        for row in reference_platform.warehouse.query(
            "select table_name from information_schema.tables "
            "where table_schema = 'main'"
        )
    }
    assert "lake_cdc_events" in relations
    assert not any(name in relations for name in TRACKED_TABLES)
