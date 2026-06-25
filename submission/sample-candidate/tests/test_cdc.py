"""
Tests — CDC capture correctness.

Covers: insert/update/delete capture, invalid operation rejection,
sequence monotonicity, checkpoint-based replay, duplicate safety.
"""

from datetime import datetime, timezone

import pytest

from pipeline.cdc import CDCCapture, CDCRecord


def _ts() -> datetime:
    return datetime.now(timezone.utc)


# ── insert ────────────────────────────────────────────────────────────────────


def test_insert_creates_record_with_correct_operation():
    cap = CDCCapture()
    rec = cap.insert("customers", "c1", {"customer_id": "c1", "name": "Alice"})
    assert rec.operation == "insert"
    assert rec.table == "customers"
    assert rec.primary_key == "c1"
    assert rec.data["name"] == "Alice"


def test_insert_assigns_sequence_starting_at_one():
    cap = CDCCapture()
    rec = cap.insert("customers", "c1", {})
    assert rec.sequence == 1


# ── update ────────────────────────────────────────────────────────────────────


def test_update_creates_record_with_update_operation():
    cap = CDCCapture()
    cap.insert("customers", "c1", {"status": "active"})
    rec = cap.update("customers", "c1", {"status": "suspended"})
    assert rec.operation == "update"
    assert rec.data["status"] == "suspended"


# ── delete ────────────────────────────────────────────────────────────────────


def test_delete_creates_record_with_delete_operation():
    cap = CDCCapture()
    cap.insert("customers", "c1", {"customer_id": "c1"})
    rec = cap.delete("customers", "c1", {"customer_id": "c1"})
    assert rec.operation == "delete"
    assert rec.primary_key == "c1"


# ── invalid operation ─────────────────────────────────────────────────────────


def test_invalid_operation_raises_value_error():
    with pytest.raises(ValueError, match="Invalid CDC operation"):
        CDCRecord(operation="upsert", table="customers", primary_key="c1", data={})


# ── sequence monotonicity ─────────────────────────────────────────────────────


def test_sequences_are_strictly_increasing():
    cap = CDCCapture()
    r1 = cap.insert("customers", "c1", {})
    r2 = cap.insert("customers", "c2", {})
    r3 = cap.update("customers", "c1", {})
    assert r1.sequence < r2.sequence < r3.sequence


def test_latest_sequence_reflects_last_record():
    cap = CDCCapture()
    cap.insert("customers", "c1", {})
    cap.insert("customers", "c2", {})
    last = cap.update("customers", "c1", {})
    assert cap.latest_sequence == last.sequence


# ── checkpoint-based replay ───────────────────────────────────────────────────


def test_records_since_returns_only_records_after_offset():
    cap = CDCCapture()
    cap.insert("customers", "c1", {})
    cap.insert("customers", "c2", {})
    checkpoint = cap.latest_sequence
    r3 = cap.insert("customers", "c3", {})

    replayed = cap.records_since(checkpoint)

    assert len(replayed) == 1
    assert replayed[0].sequence == r3.sequence


def test_records_since_zero_returns_all_records():
    cap = CDCCapture()
    cap.insert("customers", "c1", {})
    cap.insert("customers", "c2", {})
    cap.delete("customers", "c1", {})
    assert len(cap.records_since(0)) == 3


def test_records_since_latest_sequence_returns_empty():
    cap = CDCCapture()
    cap.insert("customers", "c1", {})
    assert cap.records_since(cap.latest_sequence) == []


# ── duplicate / replay safety ─────────────────────────────────────────────────


def test_same_pk_can_appear_multiple_times_in_log():
    """CDC appends every event — deduplication happens downstream."""
    cap = CDCCapture()
    cap.insert("customers", "c1", {"status": "active"})
    cap.update("customers", "c1", {"status": "suspended"})
    cap.update("customers", "c1", {"status": "closed"})

    records = cap.records_since(0)
    pk_records = [r for r in records if r.primary_key == "c1"]
    assert len(pk_records) == 3


def test_captured_at_is_utc_datetime():
    cap = CDCCapture()
    rec = cap.insert("customers", "c1", {})
    assert isinstance(rec.captured_at, datetime)
    assert rec.captured_at.tzinfo is not None
