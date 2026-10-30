"""Operations: lock, init, freshness, pause/resume, recovery, config (OPS-01 … OPS-07)."""

import os

import pytest
from demo import seed_basic
from pipeline.config import load_config
from pipeline.errors import PipelineLocked


def lake_rows(wb):
    return wb.conn.execute(
        'SELECT seq, txn_id, schema_version, table_name, op, pk, "before", "after", changed_at '
        "FROM lake.cdc_events ORDER BY seq"
    ).fetchall()


def test_ops_01_second_runner_is_refused_and_stale_lease_taken_over(platform, clock):
    """OPS-01: a live lease blocks a second runner; a stale one can be taken over."""
    first = platform.pipeline
    second = platform.new_pipeline(owner="runner-2")
    first.acquire_lock()
    with pytest.raises(PipelineLocked, match="locked by 'pipeline'"):
        second.run_until_caught_up()
    with pytest.raises(PipelineLocked):
        second.run_forever(stop_after=1)
    clock.advance(3 * platform.config.poll_max_s + 1)  # heartbeat is now stale
    seed_basic(platform.app)
    result = second.run_until_caught_up()
    assert result.events > 0
    # the lease is released when the run ends, so the first runner may start again
    first.run_until_caught_up()
    second.close()


def test_ops_02_init_is_idempotent(platform):
    """OPS-02: init() twice -> no error, no change; migrations applied once."""
    seed_basic(platform.app)
    platform.sync()
    before = {
        "migrations": platform.conn.execute(
            "SELECT * FROM cdc.migrations ORDER BY version"
        ).fetchall(),
        "registry": platform.scalar("SELECT COUNT(*) FROM cdc.schema_registry"),
        "checkpoint": platform.pipeline.checkpoint(),
        "lake": platform.scalar("SELECT COUNT(*) FROM lake.cdc_events"),
        "users": platform.view("users"),
    }
    platform.pipeline.init()
    platform.pipeline.init()
    after = {
        "migrations": platform.conn.execute(
            "SELECT * FROM cdc.migrations ORDER BY version"
        ).fetchall(),
        "registry": platform.scalar("SELECT COUNT(*) FROM cdc.schema_registry"),
        "checkpoint": platform.pipeline.checkpoint(),
        "lake": platform.scalar("SELECT COUNT(*) FROM lake.cdc_events"),
        "users": platform.view("users"),
    }
    assert before == after
    assert [m[0] for m in after["migrations"]] == [1, 2, 3]
    assert platform.pipeline.status()["status"] == "RUNNING"


def test_ops_03_freshness_alerts(platform, clock):
    """OPS-03: freshness above the warn / critical threshold raises an alert."""
    ids = seed_basic(platform.app)
    platform.sync()
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM cdc.alerts WHERE level IN ('WARN','CRITICAL')"
        )
        == 0
    )
    platform.app.restock(ids["p1"], 1)
    clock.advance(90)  # > 60 s warn
    result = platform.pipeline.run_once()
    assert result.freshness_s == 0  # caught up afterwards
    warn = platform.conn.execute(
        "SELECT level, reason FROM cdc.alerts WHERE level = 'WARN'"
    ).fetchall()
    assert len(warn) == 1 and "freshness 90s exceeds 60s" in warn[0][1]
    platform.app.restock(ids["p1"], 1)
    clock.advance(400)  # > 300 s critical
    platform.pipeline.run_once()
    crit = platform.conn.execute(
        "SELECT reason FROM cdc.alerts WHERE level = 'CRITICAL'"
    ).fetchall()
    assert len(crit) == 1 and "exceeds 300s" in crit[0][0]


def test_ops_04_pause_and_resume(platform):
    """OPS-04: PAUSED -> run_once processes nothing; resume() continues."""
    ids = seed_basic(platform.app)
    platform.pipeline.pause()
    result = platform.pipeline.run_once()
    assert (result.events, result.status) == (0, "PAUSED")
    assert platform.pipeline.checkpoint() == 0
    assert platform.scalar("SELECT COUNT(*) FROM lake.cdc_events") == 0
    platform.pipeline.resume()
    assert platform.pipeline.status()["status"] == "RUNNING"
    assert platform.pipeline.run_once().events > 0
    assert ids  # seeded


def test_ops_05_recovery_from_lost_platform_file(platform):
    """OPS-05: delete platform.duckdb, re-run from checkpoint 0 -> identical data."""
    ids = seed_basic(platform.app)
    order = platform.app.place_order(ids["u1"], [(ids["p1"], 1)])
    platform.app.pay_order(order, [("wallet", 150_00, True)])
    platform.sync()
    lake_before, wh_before = lake_rows(platform), platform.warehouse()
    history_before = platform.conn.execute(
        "SELECT * FROM warehouse.orders_history ORDER BY seq"
    ).fetchall()
    platform.pipeline.close()
    for suffix in ("", ".wal"):
        path = str(platform.config.platform_db) + suffix
        if os.path.exists(path):
            os.remove(path)
    platform.pipeline = platform.new_pipeline()
    platform.pipeline.init()
    assert platform.pipeline.checkpoint() == 0
    platform.sync()
    assert lake_rows(platform) == lake_before
    assert platform.warehouse() == wh_before
    assert (
        platform.conn.execute(
            "SELECT * FROM warehouse.orders_history ORDER BY seq"
        ).fetchall()
        == history_before
    )


def test_ops_07_env_overrides_config_file(tmp_path):
    """OPS-07: config.yml values are overridden by environment variables."""
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "source_db: s.db\nplatform_db: sub/p.duckdb\ncontract_path: c.yml\n"
        "batch_size: 3\npoll_min_s: 0.5\npoll_max_s: 9\nfreshness_warn_s: 10\nfreshness_crit_s: 20\n"
    )
    plain = load_config(cfg, env={})
    assert plain.batch_size == 3 and plain.poll_max_s == 9
    assert (
        plain.platform_db == tmp_path / "sub" / "p.duckdb"
    )  # relative to the config file
    env = {
        "CDC_BATCH_SIZE": "7",
        "CDC_PLATFORM_DB": "/elsewhere/x.duckdb",
        "CDC_FRESHNESS_WARN_S": "1.5",
    }
    overridden = load_config(cfg, env=env)
    assert overridden.batch_size == 7
    assert str(overridden.platform_db) == "/elsewhere/x.duckdb"
    assert overridden.freshness_warn_s == 1.5
    assert overridden.poll_min_s == 0.5  # untouched keys keep the file value
    default = load_config(env={})
    assert (
        default.batch_size == 500
        and default.poll_min_s == 0.2
        and default.poll_max_s == 5
    )
    assert default.freshness_warn_s == 60 and default.freshness_crit_s == 300
