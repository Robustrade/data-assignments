"""CDC correctness: inserts, updates, deletes and cascades all reach the lake.

The capture mechanism is a database trigger, so the interesting failures are
the ones an application-level emitter would also have: a delete that produces
no event, a cascade that fires no trigger, an update that loses its before
image.
"""

from __future__ import annotations

import json

from cdclake import source as source_mod
from cdclake.config import OP_DELETE, OP_INSERT, OP_UPDATE


def _records(platform):
    return list(platform.lake.records())


def _by_table(platform, table, op=None):
    return [
        r
        for r in _records(platform)
        if r["table_name"] == table and (op is None or r["op"] == op)
    ]


def test_inserts_are_captured_with_an_after_image_only(seeded_platform):
    seeded_platform.ingest()
    inserts = _by_table(seeded_platform, "customer", OP_INSERT)

    assert len(inserts) == 4
    for record in inserts:
        assert record["before"] is None
        after = json.loads(record["after"])
        assert set(after) == {
            "customer_id",
            "email",
            "full_name",
            "status",
            "phone",
            "created_at",
            "updated_at",
        }


def test_updates_carry_both_images(seeded_platform):
    seeded_platform.ingest()
    source_mod.update(
        seeded_platform.conn,
        "customer",
        {"customer_id": 1},
        status="SUSPENDED",
        updated_at="2026-08-06T09:00:00.000Z",
    )
    seeded_platform.ingest()

    update = _by_table(seeded_platform, "customer", OP_UPDATE)[-1]
    assert json.loads(update["before"])["status"] == "ACTIVE"
    assert json.loads(update["after"])["status"] == "SUSPENDED"


def test_deletes_are_captured_with_a_before_image_only(seeded_platform):
    seeded_platform.ingest()
    source_mod.delete(seeded_platform.conn, "payment", payment_id=10040)
    seeded_platform.ingest()

    deletes = _by_table(seeded_platform, "payment", OP_DELETE)
    assert len(deletes) == 1
    assert deletes[0]["after"] is None
    assert json.loads(deletes[0]["before"])["payment_id"] == 10040


def test_cascaded_child_deletes_are_captured(seeded_platform):
    """The subtle one.

    SQLite does not fire delete triggers for rows removed by ON DELETE CASCADE
    unless PRAGMA recursive_triggers is on. Without it the parent delete would
    reach the lake and its children would silently vanish, leaving orphans in
    the warehouse forever.
    """
    seeded_platform.ingest()
    source_mod.delete(seeded_platform.conn, "payment", payment_id=10040)
    source_mod.delete(seeded_platform.conn, "customer_order", order_id=1004)
    seeded_platform.ingest()

    child_deletes = _by_table(seeded_platform, "order_item", OP_DELETE)
    assert child_deletes, "cascaded order_item deletes never reached the lake"
    assert json.loads(child_deletes[0]["before"])["order_id"] == 1004


def test_weak_entity_keys_are_composite_in_the_change_log(seeded_platform):
    source_mod.workload_retry_storm(seeded_platform.conn)
    seeded_platform.ingest()

    attempt = _by_table(seeded_platform, "payment_attempt", OP_INSERT)[0]
    assert set(json.loads(attempt["pk"])) == {"payment_id", "attempt_no"}
    assert attempt["pk_hash"]


def test_event_ids_are_unique_and_increase_with_commit_order(seeded_platform):
    """The replay offset has to be a real total order at the source.

    Note the lake does NOT preserve that order on disk: files are partitioned
    per table, so records() yields them table-grouped. That is fine precisely
    because nothing downstream depends on physical order — the warehouse sorts
    by event_id itself.
    """
    seeded_platform.ingest()
    source_mod.run_workload(seeded_platform.conn)
    seeded_platform.ingest()

    source_ids = [
        row[0]
        for row in seeded_platform.conn.execute(
            "SELECT event_id FROM cdc_events ORDER BY rowid"
        )
    ]
    assert source_ids == sorted(source_ids)
    assert len(source_ids) == len(set(source_ids))

    lake_ids = [r["event_id"] for r in _records(seeded_platform)]
    assert len(lake_ids) == len(set(lake_ids))
    assert set(lake_ids) == set(source_ids)

    per_table: dict[str, list[int]] = {}
    for record in _records(seeded_platform):
        per_table.setdefault(record["table_name"], []).append(record["event_id"])
    for table, ids in per_table.items():
        assert ids == sorted(ids), table


def test_untracked_changes_do_not_reach_the_lake(empty_platform):
    """The reader filters to contracted tables; nothing else can leak in."""
    empty_platform.conn.execute("CREATE TABLE scratch (id INTEGER PRIMARY KEY)")
    empty_platform.conn.execute("INSERT INTO scratch VALUES (1)")
    assert empty_platform.ingest() == 0


def test_every_batch_is_recorded_in_the_manifest(seeded_platform):
    seeded_platform.ingest()
    source_mod.workload_price_change(seeded_platform.conn)
    seeded_platform.ingest()

    batches = seeded_platform.lake.batches()
    assert len(batches) == 2
    assert batches[0]["checkpoint_after"] == batches[1]["checkpoint_before"]
    assert seeded_platform.lake.checkpoint() == batches[-1]["checkpoint_after"]


def test_metrics_are_emitted_for_every_batch(seeded_platform):
    seeded_platform.ingest()
    metrics = seeded_platform.workspace.metrics_path.read_text(encoding="utf-8")
    lines = [json.loads(line) for line in metrics.splitlines() if line.strip()]

    assert lines
    assert {"lag_p50_seconds", "lag_max_seconds", "backlog_after"} <= set(lines[0])
    assert lines[0]["backlog_after"] == 0
