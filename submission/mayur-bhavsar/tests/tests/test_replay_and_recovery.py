"""Replay, duplicates, restart and crash recovery.

These are the tests that decide whether the delivery-semantics claim in
cdclake/ingest.py is true or merely written down.
"""

from __future__ import annotations

import pytest

from cdclake import source as source_mod
from cdclake.lake import Lake, LakeRecord
from cdclake.pipeline import build_platform


def _distinct_events(platform) -> set[tuple[str, int]]:
    return {(r["table_name"], r["event_id"]) for r in platform.lake.records()}


def test_a_second_run_with_no_new_changes_is_a_no_op(seeded_platform):
    first = seeded_platform.ingest()
    assert first > 0

    assert seeded_platform.ingest() == 0
    assert len(seeded_platform.lake.batches()) == 1


def test_restart_resumes_from_the_checkpoint(workspace_root):
    """A new process must pick up exactly where the last one stopped."""
    first = build_platform(workspace_root, fresh=True, seed=True)
    first.ingest()
    checkpoint = first.lake.checkpoint()
    events_before = len(list(first.lake.records()))
    source_mod.workload_settle_payments(first.conn)
    first.close()

    # Nothing is carried over in memory: a brand new platform on the same
    # workspace, exactly as a restarted scheduler would see it.
    second = build_platform(workspace_root)
    assert second.lake.checkpoint() == checkpoint

    ingested = second.ingest()
    assert ingested > 0
    assert len(list(second.lake.records())) == events_before + ingested
    assert len(_distinct_events(second)) == events_before + ingested
    second.close()


def test_crash_between_data_write_and_manifest_loses_nothing(seeded_platform):
    """The failure mode must be a duplicate, never a loss."""
    with pytest.raises(RuntimeError, match="simulated crash"):
        seeded_platform.runner.run_batch(simulate_crash_before_manifest=True)

    # The data files landed, but no manifest entry vouches for them.
    assert seeded_platform.lake.orphan_files()
    assert seeded_platform.lake.checkpoint() == 0
    assert list(seeded_platform.lake.records()) == []

    # Restart: the same events are read again and committed properly.
    recovered = seeded_platform.ingest()
    assert recovered > 0
    assert seeded_platform.lake.checkpoint() > 0

    # Every source change is present exactly once in the manifest-visible lake.
    source_events = seeded_platform.conn.execute(
        "SELECT count(*) FROM cdc_events"
    ).fetchone()[0]
    assert len(_distinct_events(seeded_platform)) == source_events


def test_the_retry_after_a_crash_overwrites_its_own_partial_files(seeded_platform):
    """Crash residue is self-healing, by construction.

    The batch id comes from the manifest, and the crash never appended one, so
    the retry allocates the *same* id and writes the *same* file names. The
    atomic rename replaces the orphan rather than accumulating beside it.
    """
    with pytest.raises(RuntimeError):
        seeded_platform.runner.run_batch(simulate_crash_before_manifest=True)
    orphaned = {p.name for p in seeded_platform.lake.orphan_files()}
    assert orphaned

    seeded_platform.ingest()

    assert seeded_platform.lake.orphan_files() == []
    assert {p.name for p in seeded_platform.lake.manifest_files()} >= orphaned


def test_gc_removes_files_the_manifest_does_not_vouch_for(seeded_platform):
    """The residue that is not self-healing.

    If the crash and the retry straddle midnight the retry writes into a new
    ingest_date partition, leaving the first attempt behind. Those files must
    be collectable without touching history — and must never have been visible
    to a reader in the first place, because readers resolve files through the
    manifest.
    """
    seeded_platform.ingest()
    before = _distinct_events(seeded_platform)

    stray = (
        seeded_platform.workspace.lake_events_dir
        / "table_name=customer"
        / "ingest_date=1999-01-01"
        / "batch_000001.ndjson"
    )
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_text('{"event_id":-1,"table_name":"customer"}\n', encoding="utf-8")

    assert stray in seeded_platform.lake.orphan_files()
    assert _distinct_events(seeded_platform) == before  # invisible to readers

    removed = seeded_platform.lake.gc_orphans()

    assert removed == [stray]
    assert not stray.exists()
    assert _distinct_events(seeded_platform) == before


def test_duplicate_delivery_is_absorbed_by_the_warehouse(seeded_platform):
    """Write the same events twice, then prove the warehouse cannot tell."""
    seeded_platform.ingest()
    source_mod.run_workload(seeded_platform.conn)
    seeded_platform.ingest()
    seeded_platform.refresh_warehouse()

    clean = seeded_platform.warehouse.row_counts()
    staged = seeded_platform.warehouse.query(
        "select count(*) as n from stg_cdc_events"
    )[0]["n"]

    # Replay every record into a brand new batch: a textbook at-least-once
    # redelivery.
    records = [LakeRecord(**r) for r in seeded_platform.lake.records()]
    lake = Lake(seeded_platform.workspace)
    lake.write_batch(
        records,
        batch_id=lake.next_batch_id(),
        checkpoint_before=lake.checkpoint(),
        started_at="2026-08-29T00:00:00.000Z",
    )
    seeded_platform.refresh_warehouse()

    assert seeded_platform.warehouse.row_counts() == clean
    assert (
        seeded_platform.warehouse.query("select count(*) as n from stg_cdc_events")[0][
            "n"
        ]
        == staged
    )


def test_out_of_order_arrival_produces_the_same_warehouse(
    seeded_platform, workspace_root
):
    """Ordering is by version, not arrival.

    The same change records, landed in reverse order in a different workspace,
    must produce a byte-identical current state. This is what "the warehouse is
    a pure function of the lake" means operationally.
    """
    seeded_platform.ingest()
    source_mod.run_workload(seeded_platform.conn)
    seeded_platform.ingest()
    seeded_platform.refresh_warehouse()
    expected = {
        entity: seeded_platform.warehouse.current(entity)
        for entity in ("customer", "customer_order", "order_item", "payment")
    }

    shuffled = build_platform(workspace_root.parent / "shuffled", fresh=True)
    records = [LakeRecord(**r) for r in seeded_platform.lake.records()]
    lake = Lake(shuffled.workspace)
    lake.write_batch(
        list(reversed(records)),
        batch_id=1,
        checkpoint_before=0,
        started_at="2026-08-29T00:00:00.000Z",
    )
    shuffled.refresh_warehouse()

    for entity, rows in expected.items():
        assert shuffled.warehouse.current(entity) == rows, entity
    shuffled.close()


def test_full_replay_from_zero_reproduces_the_warehouse(seeded_platform):
    """Recovery from history: rebuild everything from the change log alone."""
    seeded_platform.ingest()
    source_mod.run_workload(seeded_platform.conn)
    seeded_platform.ingest()
    seeded_platform.refresh_warehouse()
    before = {
        entity: seeded_platform.warehouse.current(entity)
        for entity in ("customer", "customer_order", "payment")
    }

    # Drop the warehouse entirely and rebuild from the lake.
    seeded_platform.workspace.warehouse_db.unlink()
    seeded_platform.refresh_warehouse()

    for entity, rows in before.items():
        assert seeded_platform.warehouse.current(entity) == rows, entity
