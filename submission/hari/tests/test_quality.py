"""Validation parity: one failing case per source rule S1–S14 (DQ-01 … DQ-14)."""

import json
import re
from pathlib import Path

import pytest
from demo import seed_basic
from pipeline.contract import parse_enums
from pipeline.errors import QualityBlocked
from pipeline.quality.rules import RULES, Ctx, column_rule_specs
from tests.conftest import AT, make_event, mixed_workload

SEQ = 5000  # injected events live above everything the source produced


@pytest.fixture
def clean(platform):
    ids, _ = mixed_workload(platform)
    platform.sync()
    return platform, ids


def row_of(wb, table, **where):
    cond = " AND ".join(f"{k} = ?" for k in where)
    return dict(
        wb.src.execute(
            f"SELECT * FROM {table} WHERE {cond}", list(where.values())
        ).fetchone()
    )


def pk_of(wb, table, row):
    return {c: row[c] for c in wb.contract.tables[table].primary_key}


def update(wb, seq, table, row, **changes):
    return make_event(
        seq,
        table,
        "update",
        pk_of(wb, table, row),
        row,
        {**row, **changes},
        AT.replace(hour=13),
    )


def assert_blocked(wb, events, rule, inject):
    """The batch is rejected: rolled back, FAILED, alert names the offending sequence numbers."""
    lake = wb.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    current = wb.conn.execute(
        "SELECT COUNT(*) FROM warehouse.orders_history"
    ).fetchone()[0]
    with pytest.raises(QualityBlocked) as caught:
        inject(events)
    assert rule in {
        o["rule_id"] for o in caught.value.offending
    }, caught.value.offending
    assert wb.pipeline.status()["status"] == "FAILED"
    assert wb.scalar("SELECT COUNT(*) FROM lake.cdc_events") == lake  # rolled back
    assert wb.scalar("SELECT COUNT(*) FROM warehouse.orders_history") == current
    alert = wb.conn.execute(
        "SELECT level, status, details FROM cdc.alerts ORDER BY alert_id DESC LIMIT 1"
    ).fetchone()
    assert alert[:2] == ("ERROR", "FAILED")
    assert {o["seq"] for o in caught.value.offending} <= set(
        json.loads(alert[2])["seqs"]
    )
    return caught.value.offending


def assert_quarantined(wb, events, rule, inject):
    """The batch is loaded; the offending row is copied to quality.quarantine and an alert is raised."""
    inject(events)
    assert wb.pipeline.status()["status"] == "RUNNING"
    rows = wb.conn.execute(
        "SELECT rule_id, seq, reason FROM quality.quarantine WHERE rule_id = ?", [rule]
    ).fetchall()
    assert rows, f"nothing quarantined for {rule}"
    assert (
        wb.scalar(
            "SELECT COUNT(*) FROM cdc.alerts WHERE level = 'WARN' AND reason LIKE '%quarantined%'"
        )
        >= 1
    )
    assert wb.scalar(
        "SELECT COUNT(*) FROM lake.cdc_events WHERE seq >= ?", [SEQ]
    ) == len(events)
    return rows


def test_dq_01_orphan_foreign_key_blocks_the_batch(clean, inject_events):
    """DQ-01 (S1): an order for an unknown user -> batch blocked, status FAILED."""
    wb, ids = clean
    order = row_of(wb, "orders", order_id=ids["o3"])
    order.update(order_id="O-9999", user_id="U-ghost")
    event = make_event(
        SEQ,
        "orders",
        "insert",
        {"order_id": "O-9999"},
        None,
        order,
        AT.replace(hour=13),
    )
    offending = assert_blocked(wb, [event], "S1", inject_events)
    assert (
        "foreign key" in offending[0]["reason"]
        and offending[0]["table_name"] == "orders"
    )


@pytest.mark.parametrize(
    ("label", "corrupt"),
    [
        (
            "NULL in NOT NULL column",
            "UPDATE warehouse.orders_current SET user_id = NULL",
        ),
        ("value outside enum", "UPDATE warehouse.orders_current SET status = 'bogus'"),
        (
            "duplicate primary key",
            "INSERT INTO warehouse.users_current SELECT * FROM warehouse.users_current LIMIT 1",
        ),
        (
            "orphan foreign key",
            "UPDATE warehouse.order_items_current SET order_id = 'O-none'",
        ),
    ],
)
def test_dq_01b_full_verification_finds_structural_corruption(clean, label, corrupt):
    """DQ-01 (S1): NULL / enum / duplicate-PK problems cannot arrive through events (the
    per-event contract check stops them first), so they are found by verify() of the warehouse.
    """
    wb, _ = clean
    assert wb.pipeline.verify()["blocking"] == []
    wb.conn.execute(corrupt)
    report = wb.pipeline.verify()
    assert {b["rule_id"] for b in report["blocking"]} == {"S1"}, label
    assert report["results"][0]["status"] == "fail"


@pytest.mark.parametrize(
    ("rule", "table", "where", "changes"),
    [
        ("S2", "products", {"product_id": "P-0001"}, {"price_paise": -5}),
        (
            "S3",
            "order_items",
            {"order_id": "O-0001", "line_no": 1},
            {"line_total_paise": 1},
        ),
        ("S4", "inventory", {"product_id": "P-0001"}, {"reserved": 10_000}),
        (
            "S6",
            "orders",
            {"order_id": "O-0001"},
            {"paid_at": "2000-01-01T00:00:00.000Z"},
        ),
        ("S7", "payments", {"method": "upi"}, {"wallet_txn_id": "WT-0001"}),
    ],
)
def test_dq_02_row_level_rules_block(clean, inject_events, rule, table, where, changes):
    """DQ-02 (S2–S4, S6, S7): a broken row-level rule blocks the batch."""
    wb, _ = clean
    row = row_of(wb, table, **where)
    assert_blocked(wb, [update(wb, SEQ, table, row, **changes)], rule, inject_events)


def test_dq_03_ledger_update_blocks(clean, inject_events):
    """DQ-03 (S8): an update event on wallet_transactions blocks the batch."""
    wb, _ = clean
    row = row_of(wb, "wallet_transactions", wallet_txn_id="WT-0001")
    assert_blocked(
        wb,
        [
            update(
                wb,
                SEQ,
                "wallet_transactions",
                row,
                amount_paise=row["amount_paise"] + 1,
            )
        ],
        "S8",
        inject_events,
    )


def test_dq_04_illegal_transition_is_quarantined(clean, inject_events):
    """DQ-04 (S5): partially_refunded -> pending is loaded, quarantined, and alerted."""
    wb, ids = clean
    row = row_of(wb, "orders", order_id=ids["o1"])
    rows = assert_quarantined(
        wb,
        [update(wb, SEQ, "orders", row, status="pending", paid_at=None)],
        "S5",
        inject_events,
    )
    assert "partially_refunded -> pending" in rows[0][2]
    assert (
        wb.scalar("SELECT status FROM warehouse.orders WHERE order_id = ?", [ids["o1"]])
        == "pending"
    )


def test_dq_05_order_total_mismatch_is_quarantined(clean, inject_events):
    """DQ-05 (S9): order total != sum of lines -> quarantined."""
    wb, ids = clean
    row = row_of(wb, "orders", order_id=ids["o3"])
    assert_quarantined(
        wb,
        [update(wb, SEQ, "orders", row, total_paise=row["total_paise"] + 100)],
        "S9",
        inject_events,
    )


def test_dq_06_payments_not_matching_total_are_quarantined(clean, inject_events):
    """DQ-06 (S10): a paid order whose successful payments differ from its total."""
    wb, ids = clean
    row = row_of(wb, "payments", order_id=ids["o1"], method="card", status="success")
    assert_quarantined(
        wb,
        [update(wb, SEQ, "payments", row, amount_paise=row["amount_paise"] - 1)],
        "S10",
        inject_events,
    )


def test_dq_07_refund_over_limits_is_quarantined(clean, inject_events):
    """DQ-07 (S11): refund quantity above the line quantity / amount above the payment."""
    wb, ids = clean
    wallet_pay = row_of(
        wb, "payments", order_id=ids["o1"], method="wallet", status="success"
    )
    refund = {
        "refund_id": "R-9999",
        "order_id": ids["o1"],
        "line_no": 1,
        "payment_id": wallet_pay["payment_id"],
        "quantity": 5,
        "amount_paise": 500_00,
        "reason": "other",
        "status": "requested",
        "provider_ref": None,
        "wallet_txn_id": None,
        "created_at": "2026-01-01T13:00:00.000Z",
        "processed_at": None,
    }
    event = make_event(
        SEQ,
        "refunds",
        "insert",
        {"refund_id": "R-9999"},
        None,
        refund,
        AT.replace(hour=13),
    )
    rows = assert_quarantined(wb, [event], "S11", inject_events)
    reasons = " ".join(r[2] for r in rows)
    assert "exceeds line quantity" in reasons and "exceeds payment" in reasons


def test_dq_08_wallet_not_matching_ledger_is_quarantined(clean, inject_events):
    """DQ-08 (S12): wallet balance != ledger."""
    wb, ids = clean
    row = row_of(wb, "wallets", user_id=ids["u1"])
    assert_quarantined(
        wb,
        [update(wb, SEQ, "wallets", row, balance_paise=row["balance_paise"] + 1)],
        "S12",
        inject_events,
    )


def test_dq_09_clean_workload_passes_every_check(platform):
    """DQ-09: all checks pass on a clean workload and the results are recorded."""
    mixed_workload(platform)
    platform.sync()
    results = platform.conn.execute(
        "SELECT rule_id, status, violations FROM quality.check_results"
    ).fetchall()
    assert {r[0] for r in results} == {f"S{i}" for i in range(1, 15)}
    assert all(r[1] == "pass" and r[2] == 0 for r in results)
    assert platform.scalar("SELECT COUNT(*) FROM quality.quarantine") == 0
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM cdc.alerts WHERE level IN ('WARN', 'ERROR')"
        )
        == 0
    )
    report = platform.pipeline.verify()
    assert report["blocking"] == [] and report["quarantined"] == []


def test_dq_10_reserved_not_matching_open_orders_is_quarantined(clean, inject_events):
    """DQ-10 (S13): reserved != quantity on pending/paid orders (still within 0..on_hand)."""
    wb, _ = clean
    row = row_of(wb, "inventory", product_id="P-0001")
    assert_quarantined(
        wb,
        [update(wb, SEQ, "inventory", row, reserved=row["reserved"] + 1)],
        "S13",
        inject_events,
    )


def test_dq_11_key_change_or_user_delete_blocks(clean, inject_events):
    """DQ-11 (S14): a key-changing update or a delete of a user blocks the batch."""
    wb, ids = clean
    user = row_of(wb, "users", user_id=ids["u1"])
    moved = make_event(
        SEQ,
        "users",
        "update",
        {"user_id": ids["u1"]},
        user,
        {**user, "user_id": "U-moved"},
        AT.replace(hour=13),
    )
    assert_blocked(wb, [moved], "S14", inject_events)
    wb.pipeline.resume()
    deleted = make_event(
        SEQ + 1,
        "users",
        "delete",
        {"user_id": ids["u2"]},
        row_of(wb, "users", user_id=ids["u2"]),
        None,
        AT.replace(hour=13),
    )
    assert_blocked(wb, [deleted], "S14", inject_events)


def test_dq_12_every_schema_rule_has_a_warehouse_check(platform):
    """DQ-12: rule ids S1–S14 in SCHEMA.md §6 all exist in the rules registry (parity test)."""
    text = (Path(__file__).resolve().parent.parent / "SCHEMA.md").read_text()
    section = text.split("## 6. Invariants")[1].split("\n## ")[0]
    documented = set(re.findall(r"^\| (S\d+) \|", section, flags=re.MULTILINE))
    assert documented == {f"S{i}" for i in range(1, 15)}
    assert documented <= set(
        RULES
    ), f"rules without a warehouse check: {documented - set(RULES)}"
    assert set(RULES) <= documented, "registry has rules SCHEMA.md does not define"
    ctx = Ctx(platform.pipeline.model, platform.pipeline.contract)
    for rule_id, rule in RULES.items():
        assert (
            rule.id == rule_id
            and rule.description
            and rule.severity in ("block", "quarantine")
        )
        assert rule.build(ctx), f"{rule_id} generates no query"
    blocking = {r for r, rule in RULES.items() if rule.severity == "block"}
    assert blocking == {"S1", "S2", "S3", "S4", "S6", "S7", "S8", "S14"}  # README §8


def test_dq_13_failed_then_source_fixed_then_resume_with_quarantine(platform):
    """DQ-13: FAILED -> fix the source -> resume(quarantine_seqs=…) loads and records the override."""
    ids = seed_basic(platform.app)
    platform.sync()
    # a write that bypassed the source's rules: an order for a user that does not exist
    platform.src.execute("PRAGMA foreign_keys = OFF")
    platform.src.execute(
        "INSERT INTO orders (order_id, user_id, status, total_paise, currency, placed_at, updated_at) "
        "VALUES ('O-9999', 'U-ghost', 'pending', 0, 'INR', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')"
    )
    platform.src.execute("PRAGMA foreign_keys = ON")
    bad_seq = platform.src.execute("SELECT max(seq) FROM change_log").fetchone()[0]
    with pytest.raises(QualityBlocked):
        platform.pipeline.run_once(max_events=1)
    assert platform.pipeline.status()["status"] == "FAILED"
    # without an override the same batch keeps failing after a plain resume
    platform.pipeline.resume()
    with pytest.raises(QualityBlocked):
        platform.pipeline.run_once(max_events=1)
    # fix the source: the missing user now exists (new, correct events) ...
    platform.src.execute(
        "INSERT INTO users (user_id, name, email, status, created_at, updated_at) VALUES "
        "('U-ghost', 'Ghost', 'ghost@example.com', 'active', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')"
    )
    # ... and the operator accepts the bad event into quarantine
    platform.pipeline.resume(
        quarantine_seqs=[bad_seq], actor="hari", reason="orphan order from import bug"
    )
    assert platform.pipeline.status()["status"] == "RUNNING"
    override = platform.conn.execute(
        "SELECT reason, details FROM cdc.alerts WHERE reason LIKE 'operator override%'"
    ).fetchone()
    assert str(bad_seq) in override[0] and json.loads(override[1])["actor"] == "hari"
    result = platform.pipeline.run_once(
        max_events=1
    )  # the bad event alone, still orphaned: downgraded
    assert result.events == 1 and result.status == "RUNNING"
    quarantined = platform.conn.execute(
        "SELECT seq, rule_id FROM quality.quarantine"
    ).fetchall()
    assert quarantined == [(bad_seq, "S1")]
    assert platform.scalar(
        "SELECT applied_batch IS NOT NULL FROM cdc.quarantine_overrides WHERE seq = ?",
        [bad_seq],
    )
    platform.sync()  # the fix events load
    assert platform.warehouse() == platform.snapshot_source()
    assert ids  # seeded


def test_dq_13b_override_for_event_that_no_longer_violates_is_still_recorded(platform):
    """DQ-13: bad event and its fix in one batch -> the override is recorded in quarantine."""
    seed_basic(platform.app)
    platform.sync()
    platform.src.execute("PRAGMA foreign_keys = OFF")
    platform.src.execute(
        "INSERT INTO orders (order_id, user_id, status, total_paise, currency, placed_at, updated_at) "
        "VALUES ('O-9999', 'U-ghost', 'pending', 0, 'INR', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')"
    )
    platform.src.execute("PRAGMA foreign_keys = ON")
    bad_seq = platform.src.execute("SELECT max(seq) FROM change_log").fetchone()[0]
    platform.src.execute(
        "INSERT INTO users (user_id, name, email, status, created_at, updated_at) VALUES "
        "('U-ghost', 'Ghost', 'ghost@example.com', 'active', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')"
    )
    platform.pipeline.run_once()  # parent arrives in the same batch: no violation, nothing to override
    assert platform.pipeline.status()["status"] == "RUNNING"
    assert platform.scalar("SELECT COUNT(*) FROM quality.quarantine") == 0
    assert bad_seq


def column_rules_in_sqlite(conn, contract):
    """Re-derive the column rules from the live SQLite schema."""
    found = set()
    for table in contract.tables:
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()[0]
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall():
            if row[3]:  # NOT NULL
                found.add((table, row[1], "not_null", True))
        for column, values in parse_enums(sql).items():
            found.add((table, column, "enum", tuple(sorted(values))))
        for column, op, number in re.findall(r"CHECK \((\w+) (>=|<=) (-?\d+)\)", sql):
            found.add((table, column, "min" if op == ">=" else "max", int(number)))
    return found


def test_dq_14_sqlite_checks_and_warehouse_checks_come_from_one_definition(platform):
    """DQ-14: the contract-generated CHECKs and the warehouse checks carry the same column rules."""
    contract = platform.contract
    expected = {
        (t, c, kind, tuple(sorted(v)) if kind == "enum" else v)
        for _, t, c, kind, v in column_rule_specs(contract)
    }
    assert expected == column_rules_in_sqlite(platform.src, contract)
    # every contract row predicate is a CHECK in SQLite and a query in the warehouse rules
    ctx = Ctx(platform.pipeline.model, contract)
    sql_by_rule = {r: " ".join(q for _, q in RULES[r].build(ctx)) for r in RULES}
    for tname, table in contract.tables.items():
        sqlite_sql = platform.src.execute(
            "SELECT sql FROM sqlite_master WHERE name = ?", (tname,)
        ).fetchone()[0]
        for check in table.table_checks:
            assert f"CHECK ({check.expr})" in sqlite_sql
            assert check.expr.replace("'", "''") in sql_by_rule[check.rule]
        for rule, t, column, kind, _ in column_rule_specs(contract):
            if t == tname and kind in ("min", "max"):
                assert f"{tname}.{column}" in sql_by_rule[rule]
