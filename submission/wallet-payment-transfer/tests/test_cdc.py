"""
Tests — CDC capture correctness.

Covers:
  - insert / update / delete capture
  - sequence monotonicity
  - replay window (records_since)
  - duplicate appearance (the lake retains every event for the same PK)
  - invalid operation rejection
  - before/after image presence per operation type
  - schema-aware capture refuses writes when the source is incompatible
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.cdc import (
    CDCCapture,
    CDCRecord,
    SchemaContractViolation,
)
from pipeline.schema_detector import SchemaDetector


# ── insert / update / delete ──────────────────────────────────────────────────


def test_insert_record_carries_after_image_only(capture):
    rec = capture.insert("customers", "c1", {"customer_id": "c1"})
    assert rec.operation == "insert"
    assert rec.before is None
    assert rec.after == {"customer_id": "c1"}


def test_update_record_carries_before_and_after(capture):
    capture.insert("customers", "c1", {"customer_id": "c1", "status": "active"})
    rec = capture.update(
        "customers", "c1",
        before={"customer_id": "c1", "status": "active"},
        after={"customer_id": "c1", "status": "suspended"},
    )
    assert rec.operation == "update"
    assert rec.before["status"] == "active"
    assert rec.after["status"] == "suspended"


def test_delete_record_carries_before_image_only(capture):
    capture.insert("customers", "c1", {"customer_id": "c1"})
    rec = capture.delete("customers", "c1", before={"customer_id": "c1"})
    assert rec.operation == "delete"
    assert rec.after is None
    assert rec.before == {"customer_id": "c1"}


def test_invalid_operation_raises(capture):
    with pytest.raises(ValueError, match="Invalid CDC operation"):
        CDCRecord(
            operation="upsert",
            table="customers",
            primary_key="c1",
            before=None,
            after={"x": 1},
        )


def test_insert_without_after_raises():
    with pytest.raises(ValueError, match="must carry an 'after'"):
        CDCRecord(
            operation="insert",
            table="customers",
            primary_key="c1",
            before=None,
            after=None,
        )


def test_delete_without_before_raises():
    with pytest.raises(ValueError, match="must carry a 'before'"):
        CDCRecord(
            operation="delete",
            table="customers",
            primary_key="c1",
            before=None,
            after={"x": 1},
        )


# ── sequence monotonicity ────────────────────────────────────────────────────


def test_sequences_strictly_increasing(capture):
    r1 = capture.insert("customers", "c1", {"customer_id": "c1"})
    r2 = capture.insert("customers", "c2", {"customer_id": "c2"})
    r3 = capture.update(
        "customers", "c1",
        before={"customer_id": "c1"},
        after={"customer_id": "c1"},
    )
    assert r1.sequence < r2.sequence < r3.sequence


def test_latest_sequence_reflects_last_record(capture):
    capture.insert("customers", "c1", {"customer_id": "c1"})
    last = capture.insert("customers", "c2", {"customer_id": "c2"})
    assert capture.latest_sequence == last.sequence


# ── replay window ────────────────────────────────────────────────────────────


def test_records_since_returns_only_records_after_offset(capture):
    capture.insert("customers", "c1", {"customer_id": "c1"})
    capture.insert("customers", "c2", {"customer_id": "c2"})
    checkpoint = capture.latest_sequence
    r3 = capture.insert("customers", "c3", {"customer_id": "c3"})

    replayed = capture.records_since(checkpoint)
    assert len(replayed) == 1
    assert replayed[0].sequence == r3.sequence


def test_records_since_zero_returns_everything(capture):
    capture.insert("customers", "c1", {"customer_id": "c1"})
    capture.insert("customers", "c2", {"customer_id": "c2"})
    capture.delete("customers", "c1", before={"customer_id": "c1"})
    assert len(capture.records_since(0)) == 3


def test_records_since_latest_returns_empty(capture):
    capture.insert("customers", "c1", {"customer_id": "c1"})
    assert capture.records_since(capture.latest_sequence) == []


# ── multiple events for same PK appear in log ────────────────────────────────


def test_same_pk_can_appear_multiple_times(capture):
    """Lake-level dedup is the orchestrator's job; capture itself is append-only."""
    capture.insert(
        "customers", "c1", {"customer_id": "c1", "status": "active"}
    )
    capture.update(
        "customers", "c1",
        before={"customer_id": "c1", "status": "active"},
        after={"customer_id": "c1", "status": "suspended"},
    )
    capture.update(
        "customers", "c1",
        before={"customer_id": "c1", "status": "suspended"},
        after={"customer_id": "c1", "status": "closed"},
    )
    pk1 = [r for r in capture.log if r.primary_key == "c1"]
    assert len(pk1) == 3


# ── captured_at metadata ─────────────────────────────────────────────────────


def test_captured_at_is_utc_datetime(capture):
    rec = capture.insert("customers", "c1", {"customer_id": "c1"})
    assert isinstance(rec.captured_at, datetime)
    assert rec.captured_at.tzinfo is not None


def test_captured_at_can_be_overridden_for_replay(capture):
    fixed = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rec = capture.insert(
        "customers", "c1", {"customer_id": "c1"}, captured_at=fixed
    )
    assert rec.captured_at == fixed


# ── schema-aware capture (stop-the-line at write time) ───────────────────────


def test_capture_with_strict_schema_refuses_writes_on_drop(conn, detector):
    """If a contract column is missing on the source, every write must fail."""
    conn.execute("DROP TABLE IF EXISTS ledger_entries")
    conn.execute("DROP TABLE IF EXISTS transfer_events")
    conn.execute("DROP TABLE IF EXISTS transfers")
    conn.execute("DROP TABLE IF EXISTS payment_methods")
    conn.execute("DROP TABLE IF EXISTS wallets")
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    strict = CDCCapture(detector, require_compatible_schema=True)
    with pytest.raises(SchemaContractViolation):
        strict.insert("customers", "c1", {"customer_id": "c1"})

    assert strict.log == []
