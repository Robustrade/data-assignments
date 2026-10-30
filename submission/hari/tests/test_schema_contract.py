"""Schema change safety and evolution (SCH-01 … SCH-14)."""

import copy
import json

import pytest
from demo import seed_basic
from pipeline.contract import Contract
from pipeline.errors import SchemaContractViolation
from tests.conftest import make_event

# --------------------------------------------------------------------------- helpers


def rewrite_table_sql(conn, table, old, new):
    """Edit a table's stored CREATE statement (SQLite has no ALTER for these changes)."""
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0]
    assert old in sql, f"{old!r} not in {sql}"
    version = conn.execute("PRAGMA schema_version").fetchone()[0]
    conn.execute("PRAGMA writable_schema = ON")
    conn.execute(
        "UPDATE sqlite_master SET sql = ? WHERE type='table' AND name = ?",
        (sql.replace(old, new), table),
    )
    conn.execute(f"PRAGMA schema_version = {version + 1}")
    conn.execute("PRAGMA writable_schema = OFF")


def drop_cdc_triggers(conn, table):
    """SQLite refuses DROP COLUMN while a trigger references the column."""
    names = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND name LIKE 'cdc_%'",
        (table,),
    )
    for (name,) in names.fetchall():
        conn.execute(f"DROP TRIGGER {name}")


def drop_column(conn, table, column):
    drop_cdc_triggers(conn, table)
    conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")


def next_contract(contract, mutate):
    data = copy.deepcopy(contract.to_dict())
    data["version"] = contract.version + 1
    mutate(data)
    return Contract.from_dict(data)


@pytest.fixture
def synced(platform):
    ids = seed_basic(platform.app)
    platform.sync()
    return platform, ids


def assert_stopped(wb, kind=None, column=None):
    """The pipeline stops, records an alert, and writes nothing for the batch."""
    lake_before = wb.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    checkpoint = wb.pipeline.checkpoint()
    with pytest.raises(SchemaContractViolation) as caught:
        wb.pipeline.run_once()
    status = wb.pipeline.status()
    assert status["status"] == "STOPPED"
    assert wb.scalar("SELECT COUNT(*) FROM lake.cdc_events") == lake_before
    assert wb.pipeline.checkpoint() == checkpoint
    alert = wb.conn.execute(
        "SELECT level, status, reason, details FROM cdc.alerts ORDER BY alert_id DESC LIMIT 1"
    ).fetchone()
    assert (
        alert[0] == "ERROR"
        and alert[1] == "STOPPED"
        and "schema contract violated" in alert[2]
    )
    assert json.loads(alert[3])["violations"]
    if kind:
        assert any(
            v["kind"] == kind and (column is None or v.get("column") == column)
            for v in caught.value.violations
        ), caught.value.violations
    return caught.value


# --------------------------------------------------------------------------- detection (Phase 3)


def test_sch_01_dropped_column_stops_the_pipeline(synced):
    """SCH-01: drop a column -> STOPPED, alert, nothing written for the batch (even valid events)."""
    wb, ids = synced
    wb.app.restock(ids["p1"], 1)  # a valid event captured before the change is held too
    drop_column(wb.src, "users", "phone")
    assert_stopped(wb, "column_missing", "phone")


def test_sch_02_renamed_column_stops_the_pipeline(synced):
    """SCH-02: rename a column -> STOPPED (seen as drop + add); SQLite rewrites the trigger bodies."""
    wb, ids = synced
    wb.src.execute("ALTER TABLE users RENAME COLUMN phone TO mobile")
    wb.src.execute(
        "UPDATE users SET mobile = '+912222222222' WHERE user_id = ?", (ids["u1"],)
    )
    event = wb.src.execute(
        'SELECT "after" FROM change_log ORDER BY seq DESC LIMIT 1'
    ).fetchone()[0]
    assert (
        '"phone"' in event and "mobile" not in event
    )  # payloads cannot reveal the rename
    error = assert_stopped(wb, "column_missing", "phone")
    assert not any(
        v["kind"] == "column_added" for v in error.violations
    )  # nullable add is only a warning


def test_sch_03_type_change_stops_the_pipeline(synced):
    """SCH-03: change a column type -> STOPPED."""
    wb, _ = synced
    rewrite_table_sql(wb.src, "products", "price_paise INTEGER", "price_paise TEXT")
    assert_stopped(wb, "type_changed", "price_paise")


@pytest.mark.parametrize(
    ("table", "old", "new", "column"),
    [
        ("users", "phone TEXT,", "phone TEXT NOT NULL,", "phone"),
        ("users", "name TEXT NOT NULL", "name TEXT", "name"),
    ],
)
def test_sch_04_nullability_change_stops_the_pipeline(synced, table, old, new, column):
    """SCH-04: NULL <-> NOT NULL -> STOPPED."""
    wb, _ = synced
    rewrite_table_sql(wb.src, table, old, new)
    assert_stopped(wb, "nullability_changed", column)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("'suspended', 'closed'", "'suspended', 'closed', 'archived'"),
        ("'active', 'suspended', 'closed'", "'active', 'closed'"),
    ],
)
def test_sch_05_enum_change_stops_the_pipeline(synced, old, new):
    """SCH-05: add or remove an enum value in a CHECK -> STOPPED."""
    wb, _ = synced
    rewrite_table_sql(wb.src, "users", old, new)
    assert_stopped(wb, "enum_changed", "status")


def test_sch_06_event_with_value_outside_the_contract_enum(synced, inject_events):
    """SCH-06: an event carrying an unknown enum value -> STOPPED (per-event check)."""
    wb, ids = synced
    bad = {
        **dict(
            wb.src.execute(
                "SELECT * FROM users WHERE user_id=?", (ids["u1"],)
            ).fetchone()
        ),
        "status": "banned",
    }
    lake_before = wb.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    with pytest.raises(SchemaContractViolation, match="not in"):
        inject_events(
            [make_event(900, "users", "update", {"user_id": ids["u1"]}, None, bad)]
        )
    assert wb.pipeline.status()["status"] == "STOPPED"
    assert wb.scalar("SELECT COUNT(*) FROM lake.cdc_events") == lake_before


def test_sch_06b_event_from_a_source_that_bypassed_its_checks(synced):
    """SCH-06: a real captured event with a bad value (CHECKs switched off) stops the run."""
    wb, ids = synced
    wb.src.execute("PRAGMA ignore_check_constraints = ON")
    wb.src.execute("UPDATE users SET status = 'banned' WHERE user_id = ?", (ids["u1"],))
    wb.src.execute("PRAGMA ignore_check_constraints = OFF")
    assert_stopped(wb)
    assert "banned" in wb.pipeline.status()["reason"]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("PRIMARY KEY (order_id)", "PRIMARY KEY (order_id, user_id)"),
        (
            "FOREIGN KEY (user_id) REFERENCES users(user_id)",
            "FOREIGN KEY (user_id) REFERENCES wallets(wallet_id)",
        ),
    ],
)
def test_sch_07_key_change_stops_the_pipeline(synced, old, new):
    """SCH-07: change a primary or foreign key -> STOPPED."""
    wb, _ = synced
    rewrite_table_sql(wb.src, "orders", old, new)
    assert_stopped(
        wb,
        "primary_key_changed" if old.startswith("PRIMARY") else "foreign_key_changed",
    )


def test_sch_08_dropped_table_or_cdc_trigger_stops_the_pipeline(make_workbench):
    """SCH-08: drop a table / a CDC trigger -> STOPPED."""
    one = make_workbench("t1")
    seed_basic(one.app)
    one.sync()
    one.src.execute("DROP TABLE refunds")
    assert_stopped(one, "table_dropped", None)
    two = make_workbench("t2")
    seed_basic(two.app)
    two.sync()
    two.src.execute("DROP TRIGGER cdc_orders_upd")
    assert_stopped(two, "trigger_missing")


def test_sch_09_additive_changes_only_warn(synced):
    """SCH-09: new nullable column / new table -> warning, ingestion continues, column ignored."""
    wb, _ids = synced
    wb.src.execute("ALTER TABLE users ADD COLUMN nickname TEXT")
    wb.src.execute("CREATE TABLE audit_notes (note_id INTEGER PRIMARY KEY, body TEXT)")
    wb.app.create_user("Nick", "nick@example.com")
    result = wb.pipeline.run_once()
    assert result.events > 0 and result.status == "RUNNING"
    warnings = wb.conn.execute(
        "SELECT level, reason FROM cdc.alerts WHERE level = 'WARN'"
    ).fetchall()
    assert (
        len(warnings) == 1
        and "nickname" in warnings[0][1]
        and "audit_notes" in warnings[0][1]
    )
    wb.app.create_user("Nick2", "nick2@example.com")
    wb.pipeline.run_once()
    assert (
        wb.scalar("SELECT COUNT(*) FROM cdc.alerts WHERE level = 'WARN'") == 1
    )  # not repeated
    cols = {
        r[0]
        for r in wb.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'users_current'"
        ).fetchall()
    }
    assert "nickname" not in cols
    assert wb.warehouse() == wb.snapshot_source()


def test_sch_10_run_once_is_a_no_op_while_stopped(synced):
    """SCH-10: while STOPPED, run_once() does nothing and the status is unchanged."""
    wb, _ = synced
    rewrite_table_sql(wb.src, "products", "price_paise INTEGER", "price_paise TEXT")
    assert_stopped(wb)
    alerts = wb.scalar("SELECT COUNT(*) FROM cdc.alerts")
    since = wb.pipeline.status()["since"]
    result = wb.pipeline.run_once()
    assert (result.events, result.status) == (0, "STOPPED")
    assert wb.scalar("SELECT COUNT(*) FROM cdc.alerts") == alerts
    assert wb.pipeline.status()["since"] == since
    assert wb.pipeline.run_forever(stop_after=3) == "STOPPED"
    with pytest.raises(SchemaContractViolation):  # still broken, so resume() refuses
        wb.pipeline.resume()
    assert wb.pipeline.status()["status"] == "STOPPED"


def test_sch_13_unregistered_schema_version_stops_the_pipeline(synced, inject_events):
    """SCH-13: an event stamped with an unknown schema_version -> STOPPED."""
    wb, ids = synced
    row = dict(
        wb.src.execute("SELECT * FROM users WHERE user_id=?", (ids["u1"],)).fetchone()
    )
    with pytest.raises(SchemaContractViolation, match="unregistered schema_version 7"):
        inject_events(
            [
                make_event(
                    901, "users", "update", {"user_id": ids["u1"]}, None, row, version=7
                )
            ]
        )
    assert wb.pipeline.status()["status"] == "STOPPED"


# --------------------------------------------------------------------------- evolution (Phase 7)


def insert_user_without_phone(wb, user_id, email):
    """The application is redeployed for the new schema; here a direct write stands in."""
    wb.src.execute(
        "INSERT INTO users (user_id, name, email, status, created_at, updated_at) "
        "VALUES (?, 'New', ?, 'active', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')",
        (user_id, email),
    )


def project(rows, keys):
    return [{k: r[k] for k in keys} for r in rows]


def drop_phone(data):
    del data["tables"]["users"]["columns"]["phone"]
    data["tables"]["users"]["pii"] = ["email"]


def test_sch_11_approve_new_contract_and_resume(synced):
    """SCH-11: update the contract, resume() -> ingestion continues from the checkpoint."""
    wb, ids = synced
    wb.app.restock(ids["p1"], 1)  # captured before the change
    drop_column(wb.src, "users", "phone")
    assert_stopped(wb)
    checkpoint = wb.pipeline.checkpoint()
    v2 = next_contract(wb.contract, drop_phone)
    with pytest.raises(SchemaContractViolation):
        wb.pipeline.resume()  # live schema matches no registered version yet
    wb.pipeline.approve_contract(v2)
    wb.pipeline.resume()
    assert wb.pipeline.status()["status"] == "RUNNING"
    insert_user_without_phone(
        wb, "U-0900", "post@example.com"
    )  # captured by the regenerated v2 triggers
    wb.sync()
    seqs = [
        r[0]
        for r in wb.conn.execute(
            "SELECT seq FROM lake.cdc_events ORDER BY seq"
        ).fetchall()
    ]
    assert seqs == [
        r[0]
        for r in wb.src.execute("SELECT seq FROM change_log ORDER BY seq").fetchall()
    ]
    assert wb.pipeline.checkpoint() > checkpoint
    versions = {
        r[0]
        for r in wb.conn.execute(
            "SELECT DISTINCT schema_version FROM lake.cdc_events"
        ).fetchall()
    }
    assert versions == {1, 2}
    snapshot = wb.snapshot_source(v2)
    for table, rows in snapshot.items():
        keys = list(rows[0]) if rows else []
        assert project(wb.view(table), keys) == rows, table
    assert wb.pipeline.status()["status"] == "RUNNING"


def test_sch_12_backlog_mixing_two_contract_versions(synced):
    """SCH-12: each event is validated against the version it was captured under."""
    wb, ids = synced
    wb.app.restock(ids["p1"], 2)  # v1 events, not processed yet
    wb.app.create_user("Old", "old@example.com", None)
    wb.src.execute("ALTER TABLE users ADD COLUMN nickname TEXT NOT NULL DEFAULT 'n/a'")

    def add_nickname(data):
        data["tables"]["users"]["columns"]["nickname"] = {
            "type": "TEXT",
            "nullable": False,
        }

    v2 = next_contract(wb.contract, add_nickname)
    wb.pipeline.approve_contract(v2)
    wb.src.execute(
        "UPDATE users SET nickname = 'Ash' WHERE user_id = ?", (ids["u1"],)
    )  # a v2 event
    result = wb.sync()
    assert result.status == "RUNNING"
    versions = [
        r[0]
        for r in wb.conn.execute(
            "SELECT DISTINCT schema_version FROM lake.cdc_events ORDER BY 1"
        ).fetchall()
    ]
    assert versions == [1, 2]
    rows = wb.conn.execute(
        "SELECT user_id, nickname, _schema_version FROM warehouse.users_current ORDER BY user_id"
    ).fetchall()
    assert rows[0][1:] == ("Ash", 2)
    assert (
        rows[-1][1] is None and rows[-1][2] == 1
    )  # a v1 row has no nickname, and is not flagged as NULL
    assert wb.pipeline.status()["status"] == "RUNNING"
    assert (
        wb.scalar("SELECT COUNT(*) FROM quality.check_results WHERE status = 'fail'")
        == 0
    )


def history_snapshot(wb, table):
    return wb.conn.execute(
        f"SELECT * FROM warehouse.{table}_history ORDER BY seq"
    ).fetchall()


def test_sch_14_added_column(synced):
    """SCH-14: added column -> warehouse migrated, old history unchanged, new versions carry it."""
    wb, ids = synced
    before = history_snapshot(wb, "users")
    old_cols = [
        d[0]
        for d in wb.conn.execute(
            "SELECT * FROM warehouse.users_history LIMIT 0"
        ).description
    ]
    wb.src.execute("ALTER TABLE users ADD COLUMN nickname TEXT")

    def add(data):
        data["tables"]["users"]["columns"]["nickname"] = {
            "type": "TEXT",
            "nullable": True,
        }

    wb.pipeline.approve_contract(next_contract(wb.contract, add))
    wb.src.execute("UPDATE users SET nickname = 'Ash' WHERE user_id = ?", (ids["u1"],))
    wb.sync()
    new_cols = [
        d[0]
        for d in wb.conn.execute(
            "SELECT * FROM warehouse.users_history LIMIT 0"
        ).description
    ]
    assert new_cols[: len(old_cols)] == old_cols and new_cols[-1] == "nickname"
    keep = [
        i for i, c in enumerate(old_cols) if c not in ("valid_to", "is_current")
    ]  # re-chained on update
    after = history_snapshot(wb, "users")
    for old_row, new_row in zip(before, after, strict=False):
        assert [old_row[i] for i in keep] == [new_row[i] for i in keep]
        assert new_row[-1] is None  # older versions have no nickname
    assert after[-1][-1] == "Ash"
    assert (
        wb.scalar("SELECT nickname FROM warehouse.users WHERE user_id = ?", [ids["u1"]])
        == "Ash"
    )


def test_sch_14_dropped_column(synced):
    """SCH-14: dropped column stays in the warehouse (old values kept, NULL afterwards)."""
    wb, ids = synced
    old_hash = wb.scalar(
        "SELECT phone_hash FROM warehouse.users WHERE user_id = ?", [ids["u1"]]
    )
    assert old_hash
    drop_column(wb.src, "users", "phone")
    wb.pipeline.approve_contract(next_contract(wb.contract, drop_phone))
    insert_user_without_phone(wb, "U-0901", "nophone@example.com")
    wb.src.execute("UPDATE users SET name = 'Asha K' WHERE user_id = ?", (ids["u1"],))
    wb.sync()
    history = wb.conn.execute(
        "SELECT seq, phone_hash, name FROM warehouse.users_history WHERE user_id = ? ORDER BY seq",
        [ids["u1"]],
    ).fetchall()
    assert history[0][1] == old_hash  # existing history row unchanged
    assert (
        history[-1][1] is None and history[-1][2] == "Asha K"
    )  # new version uses the new shape
    assert wb.pipeline.status()["status"] == "RUNNING"


def test_sch_14_retyped_column(synced):
    """SCH-14: retyped column -> new column <col>_v2; older versions keep the old column."""
    wb, ids = synced
    rewrite_table_sql(wb.src, "products", "category TEXT", "category INTEGER")

    def retype(data):
        data["tables"]["products"]["columns"]["category"]["type"] = "INTEGER"

    wb.pipeline.approve_contract(next_contract(wb.contract, retype))
    new_id = wb.app.create_product("SKU-NEW", "Typed", 100_00, 7, 1)
    wb.sync()
    cols = {
        r[0]
        for r in wb.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'products_current'"
        ).fetchall()
    }
    assert {"category", "category_v2"} <= cols
    old = wb.conn.execute(
        "SELECT category, category_v2 FROM warehouse.products_history WHERE product_id = ?",
        [ids["p1"]],
    ).fetchone()
    assert old == ("kitchen", None)
    new = wb.conn.execute(
        "SELECT category, category_v2 FROM warehouse.products_history WHERE product_id = ?",
        [new_id],
    ).fetchone()
    assert new == (None, 7)
    assert wb.pipeline.status()["status"] == "RUNNING"
