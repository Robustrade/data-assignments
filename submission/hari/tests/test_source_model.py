"""Source model, constraints, triggers and change capture (SRC-01 … SRC-13)."""

import sqlite3

import pytest
from source.app import InsufficientStock, ShopError
from tests.conftest import expect_rejected

NOW = "2026-01-01T09:00:00.000Z"


def deliver_order(app, user, product, qty=1, method="wallet"):
    order = app.place_order(user, [(product, qty)])
    total = app.conn.execute(
        "SELECT total_paise FROM orders WHERE order_id=?", (order,)
    ).fetchone()[0]
    app.pay_order(order, [(method, total, True)])
    app.ship_order(order)
    app.deliver_order(order)
    return order


def test_src_01_keys_not_null_and_enums_are_enforced(seeded):
    """SRC-01: duplicate PK, unknown FK, NULL in NOT NULL column, value outside enum."""
    src, ids = seeded
    conn = src.conn
    expect_rejected(
        conn,
        "INSERT INTO users VALUES (?, 'x', 'x@example.com', NULL, 'active', ?, ?)",
        (ids["u1"], NOW, NOW),
    )
    expect_rejected(
        conn,
        "INSERT INTO wallets VALUES ('W-9', 'U-missing', 0, 'INR', 'active', ?, ?)",
        (NOW, NOW),
    )
    expect_rejected(
        conn,
        "INSERT INTO users VALUES ('U-9', NULL, 'n@example.com', NULL, 'active', ?, ?)",
        (NOW, NOW),
    )
    expect_rejected(
        conn,
        "INSERT INTO users VALUES ('U-9', 'n', 'n@example.com', NULL, 'banned', ?, ?)",
        (NOW, NOW),
    )
    expect_rejected(  # unique email
        conn,
        "INSERT INTO users VALUES ('U-9', 'n', 'asha@example.com', NULL, 'active', ?, ?)",
        (NOW, NOW),
    )


def test_src_02_amount_and_row_rules_are_enforced(seeded):
    """SRC-02: negative amount / balance, reserved > on_hand, wrong line_total."""
    src, ids = seeded
    conn = src.conn
    expect_rejected(
        conn,
        "INSERT INTO products VALUES ('P-9', 'S9', 'n', NULL, 0, 'active', ?, ?)",
        (NOW, NOW),
    )
    expect_rejected(
        conn, "UPDATE wallets SET balance_paise = -1 WHERE user_id = ?", (ids["u1"],)
    )
    expect_rejected(
        conn,
        "UPDATE inventory SET reserved = on_hand + 1 WHERE product_id = ?",
        (ids["p1"],),
    )
    order = src.app.place_order(ids["u1"], [(ids["p1"], 2)])
    expect_rejected(
        conn,
        "UPDATE order_items SET line_total_paise = 1 WHERE order_id = ?",
        (order,),
    )
    expect_rejected(
        conn,
        "INSERT INTO payments VALUES ('PAY-9', ?, 'card', -5, 'initiated', NULL, NULL, ?, NULL)",
        (order, NOW),
    )
    expect_rejected(  # paid_at before placed_at (S6)
        conn,
        "UPDATE orders SET paid_at = '2000-01-01T00:00:00.000Z' WHERE order_id = ?",
        (order,),
    )


def test_src_03_illegal_status_transition_is_rejected(seeded):
    """SRC-03: delivered -> pending is rejected by the lifecycle trigger."""
    src, ids = seeded
    order = deliver_order(src.app, ids["u1"], ids["p3"])
    with pytest.raises(
        sqlite3.IntegrityError, match="illegal transition orders: delivered -> pending"
    ):
        src.conn.execute(
            "UPDATE orders SET status = 'pending' WHERE order_id = ?", (order,)
        )
    pay = src.conn.execute(
        "SELECT payment_id FROM payments WHERE order_id = ?", (order,)
    ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="illegal transition payments"):
        src.conn.execute(
            "UPDATE payments SET status = 'failed' WHERE payment_id = ?", (pay,)
        )
    # a legal move still works, and re-writing the same status is not a transition
    src.conn.execute(
        "UPDATE orders SET status = 'delivered' WHERE order_id = ?", (order,)
    )


def test_src_04_wallet_ledger_is_append_only(seeded):
    """SRC-04: update or delete of a wallet_transactions row is rejected."""
    src, _ = seeded
    expect_rejected(src.conn, "UPDATE wallet_transactions SET amount_paise = 5")
    expect_rejected(src.conn, "DELETE FROM wallet_transactions")


def test_src_05_payment_method_consistency(seeded):
    """SRC-05: wallet payment needs wallet_txn_id; card payment needs provider_ref."""
    src, ids = seeded
    order = src.app.place_order(ids["u1"], [(ids["p3"], 1)])
    insert = "INSERT INTO payments VALUES (?, ?, ?, 4000, 'success', ?, ?, ?, ?)"
    expect_rejected(src.conn, insert, ("PAY-9", order, "wallet", None, None, NOW, NOW))
    expect_rejected(src.conn, insert, ("PAY-9", order, "card", None, None, NOW, NOW))
    expect_rejected(
        src.conn, insert, ("PAY-9", order, "card", "REF", "WT-0001", NOW, NOW)
    )
    expect_rejected(src.conn, insert, ("PAY-9", order, "wallet", "REF", None, NOW, NOW))
    src.conn.execute(insert, ("PAY-9", order, "card", "REF", None, NOW, NOW))  # valid


def test_src_06_expected_indexes_exist(source, contract):
    """SRC-06: every index of SCHEMA.md §5 is present."""
    names = {
        r[0]
        for r in source.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
    }
    expected = {i.name for t in contract.tables.values() for i in t.indexes}
    assert expected <= names | {"ix_change_log_txn"}
    assert len(expected) == 11  # SCHEMA.md §5 lists 12 incl. change_log(txn_id)
    assert "ix_change_log_txn" in names
    plan = source.conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM orders WHERE user_id = 'U-1' AND placed_at > 'x'"
    ).fetchall()
    assert any("ix_orders_user_placed" in str(r[3]) for r in plan)


def test_src_07_every_write_on_every_table_is_captured(seeded):
    """SRC-07: insert/update/delete on all 9 tables -> exactly one change_log row each."""
    src, ids = seeded
    conn = src.conn
    order = deliver_order(src.app, ids["u1"], ids["p1"], 2)
    pay = conn.execute(
        "SELECT payment_id FROM payments WHERE order_id = ?", (order,)
    ).fetchone()[0]
    refund = src.app.request_refund(order, 1, pay, 1, 100_00, "damaged")
    cases = {
        "users": (
            "UPDATE users SET name = 'Asha K' WHERE user_id = ?",
            ids["u1"],
            False,
        ),
        "products": (
            "UPDATE products SET category = 'misc' WHERE product_id = ?",
            ids["p2"],
            True,
        ),
        "inventory": (
            "UPDATE inventory SET on_hand = on_hand + 1 WHERE product_id = ?",
            ids["p3"],
            True,
        ),
        "wallets": (
            "UPDATE wallets SET status = 'frozen' WHERE user_id = ?",
            ids["u2"],
            False,
        ),
        "orders": (
            "UPDATE orders SET updated_at = ? WHERE order_id = ?",
            (NOW, order),
            False,
        ),
        "order_items": (
            "UPDATE order_items SET quantity = quantity WHERE order_id = ?",
            order,
            False,
        ),
        "payments": (
            "UPDATE payments SET completed_at = completed_at WHERE payment_id = ?",
            pay,
            False,
        ),
        "refunds": (
            "UPDATE refunds SET reason = 'other' WHERE refund_id = ?",
            refund,
            True,
        ),
    }
    for table, (sql, param, _) in cases.items():
        before_seq = src.max_seq()
        params = param if isinstance(param, tuple) else (param,)
        conn.execute(sql, params)
        events = src.events(before_seq)
        assert len(events) == 1, table
        event = events[0]
        assert (event["table_name"], event["op"]) == (table, "update")
        assert event["before"] is not None and event["after"] is not None
        assert set(event["pk"]) == set(
            {"order_items": ["order_id", "line_no"]}.get(
                table, [event["pk"] and next(iter(event["pk"]))]
            )
        )
    # insert + delete with before/after images
    seq = src.max_seq()
    conn.execute(
        "INSERT INTO products VALUES ('P-77', 'SKU-77', 'Mug', NULL, 1000, 'active', ?, ?)",
        (NOW, NOW),
    )
    conn.execute("INSERT INTO inventory VALUES ('P-77', 3, 0, ?)", (NOW,))
    conn.execute("DELETE FROM inventory WHERE product_id = 'P-77'")
    conn.execute("DELETE FROM products WHERE product_id = 'P-77'")
    conn.execute("DELETE FROM refunds WHERE refund_id = ?", (refund,))
    events = src.events(seq)
    assert [(e["table_name"], e["op"]) for e in events] == [
        ("products", "insert"),
        ("inventory", "insert"),
        ("inventory", "delete"),
        ("products", "delete"),
        ("refunds", "delete"),
    ]
    assert events[0]["before"] is None and events[0]["after"]["sku"] == "SKU-77"
    assert events[3]["after"] is None and events[3]["before"]["sku"] == "SKU-77"
    assert events[3]["pk"] == {"product_id": "P-77"}
    # every table was inserted into by the seed + order flow
    seen = {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT table_name FROM change_log WHERE op = 'insert'"
        )
    }
    assert len(seen) == 9


def test_src_08_one_helper_one_transaction_id(seeded):
    """SRC-08: all events of one write helper share a txn_id; helpers get distinct ids."""
    src, ids = seeded
    seq = src.max_seq()
    src.app.place_order(ids["u1"], [(ids["p1"], 1), (ids["p2"], 1)])
    first = src.events(seq)
    assert [e["table_name"] for e in first] == [
        "orders",
        "order_items",
        "order_items",
        "inventory",
        "inventory",
    ]
    assert len({e["txn_id"] for e in first}) == 1
    seq2 = src.max_seq()
    src.app.place_order(ids["u2"], [(ids["p3"], 1)])
    second = src.events(seq2)
    assert len({e["txn_id"] for e in second}) == 1
    assert first[0]["txn_id"] != second[0]["txn_id"]
    assert not first[0]["txn_id"].startswith("auto-")


def test_src_08b_every_helper_groups_its_events(seeded):
    """Phase-1 done criterion: each app helper yields exactly one txn_id."""
    src, ids = seeded
    app = src.app
    calls = [
        lambda: app.create_user("New", "new@example.com"),
        lambda: app.topup_wallet(ids["u1"], 1000),
        lambda: app.create_product("SKU-X", "X", 500, None, 4),
        lambda: app.restock(ids["p1"], 2),
        lambda: app.update_product_price(ids["p1"], 160_00),
    ]
    order_box = {}
    calls.append(
        lambda: order_box.setdefault(
            "o", app.place_order(ids["u1"], [(ids["p1"], 1), (ids["p2"], 2)])
        )
    )
    calls.append(lambda: app.remove_order_line(order_box["o"], 2))
    calls.append(
        lambda: app.pay_order(
            order_box["o"],
            [("upi", 1000, False), ("wallet", 100_00, True), ("card", 60_00, True)],
        )
    )
    calls.append(lambda: app.ship_order(order_box["o"]))
    calls.append(lambda: app.deliver_order(order_box["o"]))
    pay_box = {}

    def request():
        pay = src.conn.execute(
            "SELECT payment_id FROM payments WHERE order_id=? AND method='wallet' AND status='success'",
            (order_box["o"],),
        ).fetchone()[0]
        pay_box["r"] = app.request_refund(order_box["o"], 1, pay, 1, 100_00, "damaged")

    calls += [
        request,
        lambda: app.approve_refund(pay_box["r"]),
        lambda: app.process_refund(pay_box["r"]),
    ]
    calls += [
        lambda: app.discontinue_product(ids["p3"]),
        lambda: app.close_user(ids["u2"]),
    ]
    for call in calls:
        seq = src.max_seq()
        call()
        events = src.events(seq)
        assert events, call
        assert len({e["txn_id"] for e in events}) == 1
    # cancel flows
    o2 = app.place_order(ids["u1"], [(ids["p1"], 1)])
    seq = src.max_seq()
    app.cancel_order(o2)
    assert len({e["txn_id"] for e in src.events(seq)}) == 1
    assert src.row("orders", order_id=order_box["o"])["status"] == "partially_refunded"


def test_src_09_primary_keys_immutable_and_no_user_wallet_delete(seeded):
    """SRC-09: PK update and user/wallet delete are rejected (S14)."""
    src, ids = seeded
    expect_rejected(
        src.conn, "UPDATE users SET user_id = 'U-99' WHERE user_id = ?", (ids["u1"],)
    )
    src.app.place_order(ids["u1"], [(ids["p1"], 1)])
    expect_rejected(src.conn, "UPDATE order_items SET line_no = 5")
    expect_rejected(src.conn, "DELETE FROM users WHERE user_id = ?", (ids["u2"],))
    expect_rejected(src.conn, "DELETE FROM wallets")


def stock(src, product):
    r = src.row("inventory", product_id=product)
    return r["on_hand"], r["reserved"]


def test_src_10_inventory_flow(seeded):
    """SRC-10: reserved/on_hand follow SCHEMA.md; over-stock orders are rejected."""
    src, ids = seeded
    app = src.app
    assert stock(src, ids["p2"]) == (5, 0)
    order = app.place_order(ids["u1"], [(ids["p2"], 2)])
    assert stock(src, ids["p2"]) == (5, 2)
    app.cancel_order(order)
    assert stock(src, ids["p2"]) == (5, 0)
    order = app.place_order(ids["u1"], [(ids["p2"], 3)])
    app.pay_order(order, [("wallet", 270_00, True)])
    app.ship_order(order)
    assert stock(src, ids["p2"]) == (2, 0)
    seq = src.max_seq()
    with pytest.raises(InsufficientStock):
        app.place_order(ids["u1"], [(ids["p2"], 3)])
    assert src.max_seq() == seq  # rejected write leaves no events
    app.restock(ids["p2"], 4)
    assert stock(src, ids["p2"]) == (6, 0)
    # a refund does not restock
    app.deliver_order(order)
    pay = src.conn.execute(
        "SELECT payment_id FROM payments WHERE order_id=?", (order,)
    ).fetchone()[0]
    app.approve_refund(app.request_refund(order, 1, pay, 1, 90_00, "damaged"))
    assert stock(src, ids["p2"]) == (6, 0)
    # removing a line releases its stock (S13)
    o2 = app.place_order(ids["u2"], [(ids["p1"], 1), (ids["p3"], 2)])
    app.remove_order_line(o2, 2)
    assert stock(src, ids["p3"]) == (3, 0)
    assert stock(src, ids["p1"]) == (10, 1)


def test_src_11_delete_never_ordered_product(seeded):
    """SRC-11: product and inventory deleted together; ordered products cannot be."""
    src, ids = seeded
    seq = src.max_seq()
    src.app.delete_product(ids["p3"])
    events = src.events(seq)
    assert [(e["table_name"], e["op"]) for e in events] == [
        ("inventory", "delete"),
        ("products", "delete"),
    ]
    assert len({e["txn_id"] for e in events}) == 1
    assert src.row("products", product_id=ids["p3"]) == {}
    src.app.place_order(ids["u1"], [(ids["p1"], 1)])
    with pytest.raises(ShopError):
        src.app.delete_product(ids["p1"])


def test_src_12_direct_write_gets_generated_txn_id(seeded):
    """SRC-12: a write that bypasses the helpers gets 'auto-<seq>'."""
    src, ids = seeded
    seq = src.max_seq()
    src.conn.execute("UPDATE users SET name = 'Direct' WHERE user_id = ?", (ids["u1"],))
    (event,) = src.events(seq)
    assert event["txn_id"] == f"auto-{event['seq']}"
    # helpers clean up the context so it cannot leak into later direct writes
    ctx = src.conn.execute('SELECT txn_id, "now" FROM _txn_context').fetchone()
    assert tuple(ctx) == (None, None)


def test_src_13_events_carry_contract_version(seeded, contract):
    """SRC-13: every event is stamped with the trigger's contract version."""
    src, _ = seeded
    versions = {e["schema_version"] for e in src.events()}
    assert versions == {contract.version}


def test_helper_timestamps_come_from_the_injected_clock(seeded, clock):
    """The fake clock reaches change_log.changed_at through _txn_context."""
    src, ids = seeded
    clock.advance(3600)
    seq = src.max_seq()
    src.app.restock(ids["p1"], 1)
    (event,) = src.events(seq)
    assert event["changed_at"] == clock.iso()
    assert event["after"]["updated_at"] == clock.iso()
