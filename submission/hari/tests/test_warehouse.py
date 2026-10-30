"""Warehouse correctness: parity with the source, history, time travel, restore (WH-01 … WH-09)."""

import hashlib
import itertools

import pytest
from demo import seed_basic
from tests.conftest import mixed_workload


def raw_dump(wb):
    """Every warehouse table, current and history, as plain tuples."""
    out = {}
    for table in wb.contract.tables:
        pk = ", ".join(wb.contract.tables[table].primary_key)
        out[f"{table}_current"] = wb.conn.execute(
            f"SELECT * FROM warehouse.{table}_current ORDER BY {pk}"
        ).fetchall()
        out[f"{table}_history"] = wb.conn.execute(
            f"SELECT * FROM warehouse.{table}_history ORDER BY seq"
        ).fetchall()
    return out


def test_wh_01_warehouse_equals_source_after_mixed_workload(platform):
    """WH-01: warehouse views == source snapshot for all 9 tables."""
    mixed_workload(platform)
    platform.sync()
    snapshot, warehouse = platform.snapshot_source(), platform.warehouse()
    assert set(snapshot) == set(platform.contract.tables) and len(snapshot) == 9
    for table in snapshot:
        assert warehouse[table] == snapshot[table], table
    assert (
        len(snapshot["refunds"]) == 3
        and snapshot["orders"][0]["status"] == "partially_refunded"
    )


def test_wh_02_rebuild_from_the_lake_equals_incremental(platform):
    """WH-02: replaying the whole lake rebuilds the same current state and history."""
    mixed_workload(platform)
    platform.sync()
    incremental = raw_dump(platform)
    platform.pipeline.pause()
    replayed = platform.pipeline.rebuild()
    assert replayed == platform.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    assert raw_dump(platform) == incremental
    assert platform.pipeline.status()["status"] == "PAUSED"
    platform.pipeline.resume()
    assert platform.sync().events == 0


def test_wh_02b_rebuild_requires_a_paused_pipeline(platform):
    """Runbook guard: rebuilding while the pipeline is running is refused."""
    seed_basic(platform.app)
    platform.sync()
    with pytest.raises(Exception, match="pause"):
        platform.pipeline.rebuild()


def test_wh_03_as_of_matches_the_source_at_that_time(platform):
    """WH-03: as_of(t) equals the source snapshot taken at t."""
    _, marks = mixed_workload(platform)
    platform.sync()
    assert len(marks) >= 10
    for moment, snapshot in marks:
        assert platform.pipeline.as_of(moment) == snapshot, moment
    # before anything existed
    first = marks[0][0].replace(year=2025)
    assert all(rows == [] for rows in platform.pipeline.as_of(first).values())


def test_wh_04_history_has_one_version_per_change(platform):
    """WH-04: one version per change; valid_to == next valid_from; one current version."""
    ids, _ = mixed_workload(platform)
    platform.sync()
    events = platform.conn.execute(
        "SELECT seq FROM lake.cdc_events WHERE table_name = 'orders' AND pk = ? ORDER BY seq",
        ['{{"order_id":"{}"}}'.format(ids["o1"])],
    ).fetchall()
    versions = platform.conn.execute(
        "SELECT seq, status, valid_from, valid_to, is_current FROM warehouse.orders_history "
        "WHERE order_id = ? ORDER BY seq",
        [ids["o1"]],
    ).fetchall()
    assert [v[0] for v in versions] == [e[0] for e in events]
    for current, following in itertools.pairwise(versions):
        assert current[3] == following[2] and current[4] is False
    assert versions[-1][3] is None and versions[-1][4] is True
    assert sum(1 for v in versions if v[4]) == 1
    statuses = [v[1] for v in versions]
    assert (
        statuses[:4] == ["pending", "paid", "shipped", "delivered"]
        and statuses[-1] == "partially_refunded"
    )


def test_wh_05_rebuild_to_a_point_in_time(platform):
    """WH-05: rebuild_to(t) -> current == state at t; checkpoint == last seq <= t."""
    _, marks = mixed_workload(platform)
    platform.sync()
    final = platform.snapshot_source()
    moment, snapshot = marks[6]
    platform.pipeline.pause()
    last = platform.pipeline.rebuild_to(ts=moment)
    expected = platform.scalar(
        "SELECT max(seq) FROM lake.cdc_events WHERE changed_at <= ?",
        [moment.replace(tzinfo=None)],
    )
    assert last == expected == platform.pipeline.checkpoint()
    assert platform.warehouse() == snapshot
    assert platform.pipeline.status()["status"] == "PAUSED"
    platform.pipeline.resume()
    platform.sync()  # replays forward
    assert platform.warehouse() == final
    # rebuild_to a sequence number that lands inside a transaction is completed to its end
    inside = platform.conn.execute(
        "SELECT min(seq) + 1 FROM lake.cdc_events WHERE txn_id = (SELECT txn_id FROM lake.cdc_events "
        "WHERE table_name = 'order_items' ORDER BY seq LIMIT 1)"
    ).fetchone()[0]
    platform.pipeline.pause()
    end = platform.pipeline.rebuild_to(seq=inside)
    txn_end = platform.scalar(
        "SELECT max(seq) FROM lake.cdc_events WHERE txn_id = (SELECT txn_id FROM lake.cdc_events WHERE seq = ?)",
        [inside],
    )
    assert end == txn_end >= inside


def test_wh_06_type_conversion_and_pii_hashing(platform):
    """WH-06: paise -> DECIMAL(18,2), text -> TIMESTAMP, PII hashed."""
    platform.app.create_user("Mixed", "  Mixed@Case.com ", "+91 99999 00000")
    platform.app.create_product("SKU-D", "Decimal", 12345, None, 1)
    platform.sync()
    types = {
        (r[0], r[1]): r[2]
        for r in platform.conn.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'warehouse' AND table_name LIKE '%_current'"
        ).fetchall()
    }
    assert types[("products_current", "price")] == "DECIMAL(18,2)"
    assert types[("wallets_current", "balance")] == "DECIMAL(18,2)"
    assert types[("order_items_current", "unit_price")] == "DECIMAL(18,2)"
    assert types[("users_current", "created_at")] == "TIMESTAMP"
    assert types[("orders_current", "paid_at")] == "TIMESTAMP"
    assert ("products_current", "price_paise") not in types
    assert ("users_current", "email") not in types and (
        "users_current",
        "phone",
    ) not in types
    product = platform.view("products")[0]
    assert str(product["price"]) == "123.45"
    user = platform.view("users")[0]
    assert user["email_hash"] == hashlib.sha256(b"mixed@case.com").hexdigest()
    assert (
        user["phone_hash"]
        == hashlib.sha256("+91 99999 00000".lower().encode()).hexdigest()
    )
    raw = platform.conn.execute(
        "SELECT COUNT(*) FROM lake.cdc_events WHERE \"after\" LIKE '%Mixed@Case.com%'"
    ).fetchone()[0]
    assert raw == 1  # raw PII lives only in the (restricted) lake


def test_wh_07_as_of_is_consistent_across_tables(platform):
    """WH-07: at any ts the tables agree with each other (whole transactions only)."""
    _, marks = mixed_workload(platform)
    platform.sync()
    for moment, _ in marks:
        for shift in (0, 5, 29):  # also between helper calls
            state = platform.pipeline.as_of(
                moment.__class__.fromtimestamp(
                    moment.timestamp() + shift, moment.tzinfo
                )
            )
            orders = {o["order_id"]: o for o in state["orders"]}
            for order_id, order in orders.items():
                lines = [i for i in state["order_items"] if i["order_id"] == order_id]
                assert order["total"] == sum(i["line_total"] for i in lines)
            for item in state["order_items"]:
                assert item["order_id"] in orders
            for product in state["inventory"]:
                open_qty = sum(
                    i["quantity"]
                    for i in state["order_items"]
                    if i["product_id"] == product["product_id"]
                    and orders[i["order_id"]]["status"] in ("pending", "paid")
                )
                assert product["reserved"] == open_qty
            for wallet in state["wallets"]:
                ledger = [
                    t
                    for t in state["wallet_transactions"]
                    if t["wallet_id"] == wallet["wallet_id"]
                ]
                assert wallet["balance"] == sum(
                    -t["amount"] if t["type"] == "payment_debit" else t["amount"]
                    for t in ledger
                )


def test_wh_08_restore_a_mistaken_source_update(platform):
    """WH-08: export_as_of -> write back through the helpers -> source restored, audit trail intact."""
    ids = seed_basic(platform.app)
    good = platform.clock.now()
    platform.sync()
    platform.clock.advance(120)
    platform.app.update_product_price(ids["p1"], 1_00)  # the mistake
    platform.sync()
    assert str(platform.view("products")[0]["price"]) == "1.00"
    rows = platform.pipeline.export_as_of("products", [ids["p1"], "P-missing"], good)
    assert [r["price_paise"] for r in rows] == [
        150_00
    ]  # unknown key omitted; raw (unhashed) source form
    platform.pipeline.pause()
    for row in rows:
        platform.app.restore_row("products", row)
    platform.pipeline.resume()
    platform.sync()
    assert (
        platform.src.execute(
            "SELECT price_paise FROM products WHERE product_id=?", (ids["p1"],)
        ).fetchone()[0]
        == 150_00
    )
    assert platform.warehouse() == platform.snapshot_source()
    prices = [
        str(r[0])
        for r in platform.conn.execute(
            "SELECT price FROM warehouse.products_history WHERE product_id = ? ORDER BY seq",
            [ids["p1"]],
        ).fetchall()
    ]
    assert prices == [
        "150.00",
        "1.00",
        "150.00",
    ]  # history shows the mistake and the fix
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM lake.cdc_events WHERE table_name = 'products'"
        )
        == 5
    )
    # composite keys and PII come back in source form as well
    platform_rows = platform.pipeline.export_as_of(
        "users", [ids["u1"]], platform.clock.now()
    )
    assert platform_rows[0]["email"] == "asha@example.com"
    mixed = platform.app.place_order(ids["u1"], [(ids["p1"], 1)])
    platform.sync()
    assert (
        platform.pipeline.export_as_of(
            "order_items", [(mixed, 1)], platform.clock.now()
        )[0]["quantity"]
        == 1
    )


def test_wh_09_rebuild_repairs_a_corrupted_warehouse(platform):
    """WH-09: after deliberately corrupting warehouse rows, rebuild() makes it equal the source again."""
    mixed_workload(platform)
    platform.sync()
    good = raw_dump(platform)
    platform.conn.execute(
        "UPDATE warehouse.orders_current SET total = 1, status = 'pending'"
    )
    platform.conn.execute("DELETE FROM warehouse.products_current")
    platform.conn.execute(
        "UPDATE warehouse.users_history SET email_hash = 'x', valid_to = NULL"
    )
    platform.conn.execute("DELETE FROM warehouse.payments_history WHERE seq % 2 = 0")
    assert platform.warehouse() != platform.snapshot_source()
    platform.pipeline.pause()
    platform.pipeline.rebuild()
    platform.pipeline.resume()
    assert platform.warehouse() == platform.snapshot_source()
    assert raw_dump(platform) == good
