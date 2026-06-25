"""
Orchestrator — wires the layers together for a single pipeline tick.

A "tick" is one logical run of:
  1. read CDC events from `capture` since the last checkpoint
  2. write them to the lake (idempotent)
  3. merge them into the warehouse (idempotent)
  4. advance the checkpoint

Each step is independently safe to retry; the whole tick is therefore
safe to retry on partial failure. If the warehouse write fails before
the checkpoint moves, the next tick re-reads the same window from
capture (or, in production, from the lake) and reapplies — and the
warehouse merge silently dedups by `_cdc_seq`.
"""

from __future__ import annotations

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.checkpoint import (
    create_checkpoint_table,
    get_checkpoint,
    set_checkpoint,
)
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.schema_detector import SchemaDetector
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables


CHECKPOINT_STAGE_LAKE = "lake"
CHECKPOINT_STAGE_WAREHOUSE = "warehouse"


def bootstrap(conn: duckdb.DuckDBPyConnection) -> None:
    """Create lake, warehouse, and checkpoint tables if missing."""
    create_lake_table(conn)
    create_warehouse_tables(conn)
    create_checkpoint_table(conn)


def run_tick(
    conn: duckdb.DuckDBPyConnection,
    capture: CDCCapture,
    *,
    detector: SchemaDetector | None = None,
    fail_on_drift: bool = True,
) -> dict:
    """
    Run one orchestration tick. Returns a dict of stats:
        {'lake_written': N, 'warehouse_applied': M,
         'checkpoint_lake': X, 'checkpoint_warehouse': Y, 'aborted': bool}

    If `fail_on_drift=True` and the schema detector reports a breaking
    change on any contracted table, the tick aborts before reading any
    new events — this is the stop-the-line behavior.
    """
    bootstrap(conn)

    if detector is not None and fail_on_drift:
        report = detector.check_all()
        if report.has_breaking():
            return {
                "aborted": True,
                "reason": "schema_contract_violation",
                "drift_summary": report.summary(),
                "lake_written": 0,
                "warehouse_applied": 0,
                "checkpoint_lake": get_checkpoint(conn, CHECKPOINT_STAGE_LAKE),
                "checkpoint_warehouse": get_checkpoint(
                    conn, CHECKPOINT_STAGE_WAREHOUSE
                ),
            }

    last_lake = get_checkpoint(conn, CHECKPOINT_STAGE_LAKE)
    last_wh = get_checkpoint(conn, CHECKPOINT_STAGE_WAREHOUSE)
    floor = min(last_lake, last_wh)

    pending = capture.records_since(floor)
    if not pending:
        return {
            "aborted": False,
            "lake_written": 0,
            "warehouse_applied": 0,
            "checkpoint_lake": last_lake,
            "checkpoint_warehouse": last_wh,
        }

    # Lake first — durable storage of the raw event.
    lake_pending = [r for r in pending if r.sequence > last_lake]
    lake_written = append_to_lake(conn, lake_pending)
    new_lake_seq = pending[-1].sequence
    set_checkpoint(conn, CHECKPOINT_STAGE_LAKE, new_lake_seq)

    # Warehouse next — typed merge.
    wh_pending = [r for r in pending if r.sequence > last_wh]
    wh_applied = apply_cdc_records(conn, wh_pending)
    new_wh_seq = pending[-1].sequence
    set_checkpoint(conn, CHECKPOINT_STAGE_WAREHOUSE, new_wh_seq)

    return {
        "aborted": False,
        "lake_written": lake_written,
        "warehouse_applied": wh_applied,
        "checkpoint_lake": new_lake_seq,
        "checkpoint_warehouse": new_wh_seq,
    }
