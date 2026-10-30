"""Runtime: draining a backlog and the polling loop (RUN-01 … RUN-03)."""

from demo import seed_basic
from pipeline.clock import FakeClock
from tests.conftest import mixed_workload
from tests.test_schema_contract import drop_column


class HookClock(FakeClock):
    """A fake clock that runs a callback during chosen ``sleep`` calls (numbered from 1)."""

    def __init__(self):
        super().__init__()
        self.hooks = {}

    def sleep(self, seconds):
        super().sleep(seconds)
        hook = self.hooks.get(len(self.sleeps))
        if hook:
            hook()


def test_run_01_large_backlog_is_drained_in_several_batches(make_workbench):
    """RUN-01: run_until_caught_up processes a big backlog in batches; lag is 0 at the end."""
    wb = make_workbench("backlog", batch_size=10)
    mixed_workload(wb)
    total = wb.src.execute("SELECT COUNT(*) FROM change_log").fetchone()[0]
    assert total > 60
    result = wb.pipeline.run_until_caught_up()
    assert (
        result.batches >= 5 and result.events >= total
    )  # batches end on txn boundaries
    assert (result.lag_events, result.freshness_s, result.status) == (0, 0, "RUNNING")
    batches = wb.scalar("SELECT COUNT(DISTINCT batch_id) FROM lake.cdc_events")
    assert batches == result.batches
    assert wb.warehouse() == wb.snapshot_source()
    assert wb.scalar("SELECT COUNT(*) FROM cdc.pipeline_lock") == 0  # lease released


def test_run_02_idle_polling_backs_off_from_min_to_max(make_workbench):
    """RUN-02: with an idle source the wait grows 0.2 s -> 5 s and stays at the cap."""
    clock = HookClock()
    wb = make_workbench("idle", clock=clock)
    assert wb.pipeline.run_forever(stop_after=8) == "RUNNING"
    assert clock.sleeps == [0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0, 5.0]
    assert wb.scalar("SELECT COUNT(*) FROM cdc.pipeline_lock") == 0


def test_run_02b_back_off_resets_when_work_arrives_and_never_waits_on_a_backlog(
    make_workbench,
):
    """RUN-02: no wait while there is a backlog; the back-off restarts after new events."""
    clock = HookClock()
    wb = make_workbench("reset", clock=clock, batch_size=2)
    ids = seed_basic(
        wb.app
    )  # 14 events: several 2-event batches with no sleeping in between
    clock.hooks[3] = lambda: wb.app.restock(ids["p1"], 1)
    wb.pipeline.run_forever(stop_after=14)
    assert clock.sleeps[:3] == [0.2, 0.4, 0.8]
    assert clock.sleeps[3:6] == [0.2, 0.4, 0.8]  # reset after the restock was picked up
    assert wb.warehouse() == wb.snapshot_source()


def test_run_02c_backlog_drains_without_sleeping(make_workbench):
    """RUN-02: while lag > 0 the loop does not sleep."""
    clock = HookClock()
    wb = make_workbench("nosleep", clock=clock, batch_size=2)
    seed_basic(wb.app)
    wb.pipeline.run_forever(stop_after=4)
    assert clock.sleeps == []  # 4 cycles, each still behind


def test_run_03_breaking_change_ends_the_loop_with_stopped(make_workbench):
    """RUN-03: run_forever exits with STOPPED when a breaking change appears."""
    clock = HookClock()
    wb = make_workbench("breaking", clock=clock)
    ids = seed_basic(wb.app)
    wb.sync()

    def break_schema():
        drop_column(wb.src, "users", "phone")
        wb.app.restock(ids["p1"], 1)

    clock.hooks[2] = break_schema
    assert wb.pipeline.run_forever() == "STOPPED"
    assert wb.pipeline.status()["status"] == "STOPPED"
    assert len(clock.sleeps) == 2  # it did not keep polling
    assert wb.scalar("SELECT COUNT(*) FROM cdc.pipeline_lock") == 0
    assert (
        wb.pipeline.measure_lag()[0] == 1
    )  # the held-back event is still waiting in the source


def test_run_03b_blocking_quality_failure_ends_the_loop_with_failed(make_workbench):
    """The loop also exits on FAILED."""
    wb = make_workbench("failing")
    seed_basic(wb.app)
    wb.sync()
    wb.src.execute("PRAGMA foreign_keys = OFF")
    wb.src.execute(
        "INSERT INTO orders (order_id, user_id, status, total_paise, currency, placed_at, updated_at) "
        "VALUES ('O-9', 'U-nobody', 'pending', 0, 'INR', '2026-01-01T10:00:00.000Z', '2026-01-01T10:00:00.000Z')"
    )
    assert wb.pipeline.run_forever() == "FAILED"


def test_run_04_graceful_shutdown(make_workbench):
    """Ctrl-C and SIGTERM end the loop cleanly and release the lease."""
    clock = HookClock()
    wb = make_workbench("shutdown", clock=clock)
    clock.hooks[2] = wb.pipeline.request_stop  # what the SIGTERM handler does
    assert wb.pipeline.run_forever() == "RUNNING"
    assert (
        len(clock.sleeps) == 2
        and wb.scalar("SELECT COUNT(*) FROM cdc.pipeline_lock") == 0
    )

    def interrupt():
        raise KeyboardInterrupt

    clock2 = HookClock()
    wb2 = make_workbench("interrupt", clock=clock2)
    clock2.hooks[1] = interrupt
    assert wb2.pipeline.run_forever() == "RUNNING"
    assert wb2.scalar("SELECT COUNT(*) FROM cdc.pipeline_lock") == 0
