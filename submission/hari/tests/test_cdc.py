"""CDC correctness: capture, idempotency, crash safety, ordering, bootstrap (CDC-01 … CDC-09)."""

import pytest
from demo import Workbench, seed_basic
from pipeline.clock import FakeClock
from pipeline.errors import SimulatedCrash
from tests.conftest import AT, make_event


def lake_data(wb):
    """Lake content without ingestion metadata."""
    return wb.conn.execute(
        'SELECT seq, txn_id, schema_version, table_name, op, pk, "before", "after", changed_at '
        "FROM lake.cdc_events ORDER BY seq"
    ).fetchall()


def workload(wb):
    """A small deterministic mixed workload."""
    ids = seed_basic(wb.app)
    app = wb.app
    order = app.place_order(ids["u1"], [(ids["p1"], 2), (ids["p2"], 1)])
    app.pay_order(
        order, [("upi", 10_00, False), ("wallet", 200_00, True), ("card", 190_00, True)]
    )
    app.ship_order(order)
    wb.clock.advance(60)
    app.deliver_order(order)
    other = app.place_order(ids["u2"], [(ids["p3"], 1)])
    app.cancel_order(other)
    app.delete_product(app.create_product("SKU-9", "Temp", 1000, None, 1))
    return ids


def test_cdc_01_insert_update_delete_flow_through(platform):
    """CDC-01: three events in the lake; current reflects the end state; delete is a tombstone."""
    app = platform.app
    pid = app.create_product("SKU-A", "Gadget", 500_00, "misc", 2)
    platform.clock.advance(10)
    app.update_product_price(pid, 450_00)
    platform.clock.advance(10)
    app.delete_product(pid)
    platform.sync()
    ops = platform.conn.execute(
        "SELECT op FROM lake.cdc_events WHERE table_name = 'products' ORDER BY seq"
    ).fetchall()
    assert [o[0] for o in ops] == ["insert", "update", "delete"]
    assert platform.view("products") == []  # consumer view hides tombstones
    tomb = platform.conn.execute(
        "SELECT _is_deleted, price, _last_seq FROM warehouse.products_current"
    ).fetchone()
    assert tomb[0] is True and str(tomb[1]) == "450.00"
    assert tomb[2] == platform.scalar(
        "SELECT max(seq) FROM lake.cdc_events WHERE table_name = 'products'"
    )
    history = platform.conn.execute(
        "SELECT op, price, is_current FROM warehouse.products_history ORDER BY seq"
    ).fetchall()
    assert [(h[0], h[2]) for h in history] == [
        ("insert", False),
        ("update", False),
        ("delete", False),
    ]
    assert platform.warehouse() == platform.snapshot_source()


def test_cdc_02_processing_the_same_batch_twice_changes_nothing(platform):
    """CDC-02: re-reading the same events leaves lake and warehouse unchanged."""
    workload(platform)
    platform.sync()
    lake_before = lake_data(platform)
    wh_before = platform.warehouse()
    history_before = platform.conn.execute(
        "SELECT * FROM warehouse.orders_history ORDER BY seq"
    ).fetchall()
    current_before = platform.conn.execute(
        "SELECT * FROM warehouse.orders_current ORDER BY order_id"
    ).fetchall()
    platform.conn.execute(
        "UPDATE cdc.checkpoint SET last_seq = 0"
    )  # force a full re-read
    result = platform.sync()
    assert result.events > 0 and result.status == "RUNNING"
    assert lake_data(platform) == lake_before
    assert platform.warehouse() == wh_before
    assert (
        platform.conn.execute(
            "SELECT * FROM warehouse.orders_history ORDER BY seq"
        ).fetchall()
        == history_before
    )
    assert (
        platform.conn.execute(
            "SELECT * FROM warehouse.orders_current ORDER BY order_id"
        ).fetchall()
        == current_before
    )


@pytest.mark.parametrize("n", [0, 1, 5, 31])
def test_cdc_03_crash_mid_batch_then_restart(platform, make_workbench, fault_after, n):
    """CDC-03: crash after n events; the restart ends identical to an uninterrupted run."""
    workload(platform)
    reference = make_workbench("reference", clock=FakeClock())
    workload(reference)
    reference.sync()

    crashing = fault_after(n)
    with pytest.raises(SimulatedCrash):
        crashing.run_once()
    crashing.close()
    # the batch rolled back completely: nothing in the lake, warehouse or checkpoint
    probe = platform.new_pipeline()
    assert probe.checkpoint() == 0
    assert probe.conn.execute("SELECT COUNT(*) FROM lake.cdc_events").fetchone()[0] == 0
    assert (
        probe.conn.execute("SELECT COUNT(*) FROM warehouse.users_current").fetchone()[0]
        == 0
    )
    assert probe.status()["status"] == "RUNNING"  # a crash is not a failure verdict
    probe.close()

    platform.pipeline.close()
    platform.pipeline = platform.new_pipeline()
    platform.pipeline.init()
    platform.sync()
    assert lake_data(platform) == lake_data(reference)
    assert platform.warehouse() == reference.warehouse()
    seqs = [r[0] for r in lake_data(platform)]
    assert seqs == sorted(set(seqs))  # no duplicates
    assert seqs == [
        r[0] for r in platform.src.execute("SELECT seq FROM change_log ORDER BY seq")
    ]  # no gaps


def test_cdc_03b_failed_run_leaves_status_untouched_and_retries_cleanly(
    platform, fault_after
):
    """CDC-03: the crash is retried by the very next run (same batch re-read)."""
    workload(platform)
    first = fault_after(3)
    with pytest.raises(SimulatedCrash):
        first.run_once()
    first.close()
    platform.pipeline.close()
    platform.pipeline = platform.new_pipeline()
    result = platform.pipeline.run_once()
    assert result.events == platform.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    assert platform.warehouse() == platform.snapshot_source()


def test_cdc_04_older_event_after_newer_keeps_newer_state(platform, inject_events):
    """CDC-04: an older event arriving late is recorded but never regresses current."""
    ids = seed_basic(platform.app)
    platform.sync()
    row = platform.src.execute(
        "SELECT * FROM products WHERE product_id = ?", (ids["p1"],)
    ).fetchone()
    base = dict(row)
    newer = {**base, "price_paise": 200_00}
    older = {**base, "price_paise": 111_00}
    pk = {"product_id": ids["p1"]}
    inject_events(
        [make_event(1000, "products", "update", pk, base, newer, AT.replace(minute=10))]
    )
    inject_events(
        [make_event(999, "products", "update", pk, base, older, AT.replace(minute=5))]
    )
    current = platform.conn.execute(
        "SELECT price, _last_seq FROM warehouse.products_current WHERE product_id = ?",
        [ids["p1"]],
    ).fetchone()
    assert (str(current[0]), current[1]) == ("200.00", 1000)
    assert (
        platform.scalar("SELECT COUNT(*) FROM lake.cdc_events WHERE seq IN (999, 1000)")
        == 2
    )
    history = platform.conn.execute(
        "SELECT seq, price, valid_from, valid_to, is_current FROM warehouse.products_history "
        "WHERE product_id = ? AND seq >= 999 ORDER BY seq",
        [ids["p1"]],
    ).fetchall()
    assert [h[0] for h in history] == [999, 1000]
    assert (
        history[0][3] == history[1][2]
        and history[0][4] is False
        and history[1][4] is True
    )


def test_cdc_05_batch_never_splits_a_source_transaction(make_workbench):
    """CDC-05: a batch cut in the middle of a transaction is extended to its end."""
    wb = make_workbench("small", batch_size=3)
    ids = seed_basic(wb.app)
    wb.sync()
    order = wb.app.place_order(
        ids["u1"], [(ids["p1"], 1), (ids["p2"], 1)]
    )  # 5 events, one txn
    result = wb.pipeline.run_once(max_events=2)
    assert result.events == 5
    assert (
        wb.scalar(
            "SELECT COUNT(*) FROM warehouse.order_items WHERE order_id = ?", [order]
        )
        == 2
    )
    assert wb.warehouse() == wb.snapshot_source()
    # with a tiny batch size the checkpoint always lands on a transaction boundary
    wb.app.pay_order(order, [("wallet", 240_00, True)])
    wb.app.ship_order(order)
    while wb.pipeline.run_once(max_events=1).lag_events:
        ck = wb.pipeline.checkpoint()
        split = wb.src.execute(
            "SELECT COUNT(*) FROM change_log a JOIN change_log b ON a.txn_id = b.txn_id "
            "WHERE a.seq <= ? AND b.seq > ?",
            (ck, ck),
        ).fetchone()[0]
        assert split == 0


def test_cdc_06_restart_reads_only_events_after_the_checkpoint(platform):
    """CDC-06: with an existing checkpoint only newer events are read."""
    ids = seed_basic(platform.app)
    platform.sync()
    checkpoint = platform.pipeline.checkpoint()
    assert checkpoint > 0
    platform.app.restock(ids["p1"], 5)
    new_events = platform.src.execute(
        "SELECT COUNT(*) FROM change_log WHERE seq > ?", (checkpoint,)
    ).fetchone()[0]
    before = platform.scalar("SELECT COUNT(*) FROM lake.cdc_events")
    platform.restart()
    result = platform.pipeline.run_once()
    assert result.events == new_events == 1
    assert platform.scalar("SELECT COUNT(*) FROM lake.cdc_events") == before + 1
    assert platform.pipeline.run_once().events == 0


def test_cdc_07_late_update_to_a_deleted_row_does_not_resurrect_it(
    platform, inject_events
):
    """CDC-07: a late update leaves the row deleted but is kept in lake and history."""
    seed_basic(platform.app)
    platform.sync()
    base = {
        "product_id": "P-0099",
        "sku": "S-99",
        "name": "Ghost",
        "category": None,
        "price_paise": 1000,
        "status": "active",
        "created_at": "2026-01-01T12:00:00.000Z",
        "updated_at": "2026-01-01T12:00:00.000Z",
    }
    renamed = {**base, "name": "Ghost v2"}
    pk = {"product_id": "P-0099"}
    inject_events([make_event(1001, "products", "insert", pk, None, base, AT)])
    inject_events(
        [
            make_event(
                1003, "products", "delete", pk, renamed, None, AT.replace(minute=20)
            )
        ]
    )
    inject_events(
        [
            make_event(
                1002, "products", "update", pk, base, renamed, AT.replace(minute=10)
            )
        ]
    )
    assert platform.conn.execute(
        "SELECT _is_deleted, _last_seq FROM warehouse.products_current WHERE product_id = 'P-0099'"
    ).fetchone() == (True, 1003)
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM warehouse.products WHERE product_id = 'P-0099'"
        )
        == 0
    )
    history = platform.conn.execute(
        "SELECT seq, op, valid_to, is_current FROM warehouse.products_history WHERE product_id = 'P-0099' ORDER BY seq"
    ).fetchall()
    assert [(h[0], h[1], h[3]) for h in history] == [
        (1001, "insert", False),
        (1002, "update", False),
        (1003, "delete", False),
    ]
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM lake.cdc_events WHERE seq IN (1001, 1002, 1003)"
        )
        == 3
    )


def test_cdc_08_one_cycle_after_a_committed_change(platform):
    """CDC-08: the change is visible after one cycle; lag and freshness are 0."""
    ids = seed_basic(platform.app)
    platform.sync()
    platform.app.update_product_price(ids["p3"], 55_00)
    platform.clock.advance(5)
    result = platform.pipeline.run_once()
    assert (result.events, result.lag_events, result.freshness_s) == (1, 0, 0)
    assert str(platform.view("products")[2]["price"]) == "55.00"
    status = platform.pipeline.status()
    assert (
        status["lag_events"],
        status["freshness_s"],
        status["last_batch_events"],
    ) == (0, 0, 1)
    # a backlog shows up as lag and freshness before it is processed
    platform.app.restock(ids["p1"], 1)
    platform.app.restock(ids["p1"], 1)
    platform.clock.advance(30)
    partial = platform.pipeline.run_once(max_events=1)
    assert partial.events == 1 and partial.lag_events == 1 and partial.freshness_s == 30


def test_cdc_09_bootstrap_with_writes_during_the_snapshot(platform):
    """CDC-09: snapshot + overlap with streamed events -> warehouse == source, no duplicates."""
    ids = workload(platform)

    def writes_during_snapshot():
        platform.app.restock(ids["p1"], 7)
        platform.app.create_user("Late", "late@example.com")

    s0 = platform.pipeline.bootstrap(on_snapshot_start=writes_during_snapshot)
    assert platform.pipeline.checkpoint() == s0
    assert (
        platform.scalar("SELECT COUNT(*) FROM lake.cdc_events WHERE op = 'snapshot'")
        > 0
    )
    assert (
        platform.warehouse() == platform.snapshot_source()
    )  # snapshot already includes the writes
    result = platform.sync()  # streams from S0: re-applies the overlapping events
    assert result.events > 0
    assert platform.warehouse() == platform.snapshot_source()
    assert platform.scalar("SELECT COUNT(*) FROM lake.cdc_events") == platform.scalar(
        "SELECT COUNT(DISTINCT seq) FROM lake.cdc_events"
    )
    assert platform.scalar(
        "SELECT COUNT(*) FROM warehouse.users_history"
    ) == platform.scalar("SELECT COUNT(DISTINCT seq) FROM warehouse.users_history")
    assert platform.pipeline.status()["status"] == "RUNNING"
    # one current version per key after the replay
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM (SELECT user_id FROM warehouse.users_history WHERE is_current GROUP BY user_id HAVING count(*) > 1)"
        )
        == 0
    )


def test_cdc_09b_bootstrap_of_a_source_without_change_history(platform):
    """CDC-09: data that predates change capture is loaded from the snapshot alone."""
    seed_basic(platform.app)
    platform.src.execute(
        "DELETE FROM change_log"
    )  # as if the triggers were installed afterwards
    s0 = platform.pipeline.bootstrap()
    assert s0 == 0
    assert platform.warehouse() == platform.snapshot_source()
    platform.app.create_user("After", "after@example.com")
    platform.sync()
    assert platform.warehouse() == platform.snapshot_source()
    with pytest.raises(Exception, match="bootstrap needs an empty lake"):
        platform.pipeline.bootstrap()


def test_workbench_is_reusable(tmp_path):
    """Sanity: a workbench can be created twice in separate folders."""
    a, b = Workbench(tmp_path / "a"), Workbench(tmp_path / "b")
    assert a.config.platform_db != b.config.platform_db
    a.close()
    b.close()
