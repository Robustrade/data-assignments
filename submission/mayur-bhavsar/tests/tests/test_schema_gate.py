"""Schema change safety.

Three separate questions, each of which can fail independently:

1. Does the gate classify the change correctly?
2. Does ingestion actually stop, with nothing written?
3. Does the halt survive a restart until a human clears it?
"""

from __future__ import annotations

import json

import pytest

from cdclake import source as source_mod
from cdclake.config import CONTRACTS_DIR
from cdclake.gate import HALTED, RUNNING, IngestionHalted, PipelineState, acknowledge
from cdclake.lake import read_ndjson
from cdclake.pipeline import build_platform


@pytest.mark.parametrize("mutation", source_mod.BREAKING_MUTATIONS)
def test_breaking_change_halts_ingestion(seeded_platform, mutation):
    seeded_platform.ingest()
    checkpoint = seeded_platform.lake.checkpoint()

    source_mod.apply_mutation(seeded_platform.conn, mutation)

    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    # Nothing was written: the gate runs before a single row is read.
    assert seeded_platform.lake.checkpoint() == checkpoint
    assert seeded_platform.lake.orphan_files() == []


@pytest.mark.parametrize("mutation", source_mod.BREAKING_MUTATIONS)
def test_halt_is_persisted_with_the_reason(seeded_platform, mutation):
    seeded_platform.ingest()
    source_mod.apply_mutation(seeded_platform.conn, mutation)
    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    state = PipelineState.load(seeded_platform.workspace)
    assert state.state == HALTED
    assert state.reason
    assert state.detected_at
    assert state.contract_diff, "the halt must record what actually changed"
    assert all(d["severity"] == "BREAKING" for d in state.contract_diff)


@pytest.mark.parametrize("mutation", source_mod.BREAKING_MUTATIONS)
def test_halt_emits_an_error_alert(seeded_platform, mutation):
    seeded_platform.ingest()
    source_mod.apply_mutation(seeded_platform.conn, mutation)
    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    alerts = list(read_ndjson(seeded_platform.workspace.alerts_path))
    halts = [a for a in alerts if a["event"] == "ingestion_halted"]
    assert halts, "a breaking change must surface an alert, not just a log line"
    assert halts[-1]["severity"] == "ERROR"


def test_a_halted_pipeline_refuses_to_restart(workspace_root):
    """A scheduler must not be able to resume by simply running again."""
    platform = build_platform(workspace_root, fresh=True, seed=True)
    platform.ingest()
    source_mod.mutate_drop_column(platform.conn)
    with pytest.raises(IngestionHalted):
        platform.runner.run_batch()
    platform.close()

    restarted = build_platform(workspace_root)
    with pytest.raises(IngestionHalted, match="HALTED and has not been acknowledged"):
        restarted.runner.run_batch()
    restarted.close()


def test_acknowledgement_records_who_and_why(seeded_platform):
    seeded_platform.ingest()
    source_mod.mutate_drop_column(seeded_platform.conn)
    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    state = acknowledge(
        seeded_platform.workspace,
        operator="mayur",
        note="source team confirmed phone was retired; contract republished",
    )

    assert state.state == RUNNING
    assert state.acknowledged_by == "mayur"
    assert state.acknowledgement_note
    alerts = list(read_ndjson(seeded_platform.workspace.alerts_path))
    assert any(a["event"] == "halt_acknowledged" for a in alerts)


def test_break_glass_republish_then_resume(seeded_platform, tmp_path):
    """The full operator path out of a halt.

    Contracts are republished into a scratch directory so the test cannot
    mutate the committed ones — which is also the reason republishing is a
    deliberate command and not something the runner does on its own.
    """
    seeded_platform.ingest()
    source_mod.mutate_drop_column(seeded_platform.conn)
    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    scratch = tmp_path / "contracts"
    scratch.mkdir()
    for path in CONTRACTS_DIR.glob("*.json"):
        (scratch / path.name).write_text(path.read_text(encoding="utf-8"), "utf-8")

    from cdclake.gate import republish_contracts

    republish_contracts(seeded_platform.conn, contracts_dir=scratch)
    published = json.loads((scratch / "customer.json").read_text(encoding="utf-8"))
    assert published["contract_version"] == 2
    assert "phone" not in {c["name"] for c in published["columns"]}

    acknowledge(seeded_platform.workspace, operator="mayur", note="accepted")
    resumed = build_platform(seeded_platform.workspace.root, contracts_dir=scratch)
    assert resumed.runner.run_batch() is None  # gate passes; nothing new to read
    resumed.close()


@pytest.mark.parametrize("mutation", source_mod.COMPATIBLE_MUTATIONS)
def test_compatible_change_continues_but_warns(seeded_platform, mutation):
    seeded_platform.ingest()
    source_mod.apply_mutation(seeded_platform.conn, mutation)
    source_mod.workload_price_change(seeded_platform.conn)

    result = seeded_platform.runner.run_batch()

    assert PipelineState.load(seeded_platform.workspace).state == RUNNING
    assert result is not None and result.batch.event_count > 0

    alerts = list(read_ndjson(seeded_platform.workspace.alerts_path))
    warnings = [a for a in alerts if a["event"] == "schema_change_compatible"]
    assert warnings, "a compatible change must still be visible to an operator"
    assert warnings[-1]["severity"] == "WARN"


def test_rename_is_reported_as_indistinguishable_from_drop_plus_add(seeded_platform):
    seeded_platform.ingest()
    source_mod.mutate_rename_column(seeded_platform.conn)

    with pytest.raises(IngestionHalted):
        seeded_platform.runner.run_batch()

    diff = PipelineState.load(seeded_platform.workspace).contract_diff
    kinds = {d["kind"] for d in diff}
    assert "COLUMN_RENAMED_OR_REPLACED" in kinds
    detail = next(d for d in diff if d["kind"] == "COLUMN_RENAMED_OR_REPLACED")
    assert "indistinguishable" in detail["detail"]


def test_both_nullability_directions_are_breaking(seeded_platform):
    """Tightening breaks writers; relaxing breaks every downstream not-null."""
    from cdclake import contracts as contracts_mod

    published = contracts_mod.load_contract("product")
    live = contracts_mod.introspect_table(seeded_platform.conn, "product")

    relaxed = contracts_mod.TableContract(
        table=live.table,
        contract_version=live.contract_version,
        primary_key=live.primary_key,
        columns=tuple(
            (
                contracts_mod.ColumnSpec(c.name, c.type, True, c.enum)
                if c.name == "currency"
                else c
            )
            for c in live.columns
        ),
        foreign_keys=live.foreign_keys,
        unique_constraints=live.unique_constraints,
        indexes=live.indexes,
    )
    deltas = contracts_mod.diff_table(published, relaxed)
    assert any(
        d.kind == "NULLABILITY_RELAXED" and d.severity == "BREAKING" for d in deltas
    )

    source_mod.mutate_nullable_to_not_null(seeded_platform.conn)
    tightened = contracts_mod.introspect_table(seeded_platform.conn, "product")
    deltas = contracts_mod.diff_table(published, tightened)
    assert any(
        d.kind == "NULLABILITY_TIGHTENED" and d.severity == "BREAKING" for d in deltas
    )
