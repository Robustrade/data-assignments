"""
Tests — CDC capture correctness.
"""

from datetime import datetime
import pytest

from pipeline.cdc import CDCCapture, CDCRecord


def test_insert_creates_record_with_correct_operation():
    cap = CDCCapture()
    rec = cap.insert("trade_partners", "p1", {"partner_id": "p1", "company_name": "Test Partner"})
    assert rec.operation == "insert"
    assert rec.table == "trade_partners"
    assert rec.primary_key == "p1"
    assert rec.data["company_name"] == "Test Partner"


def test_insert_assigns_sequence_starting_at_one():
    cap = CDCCapture()
    rec = cap.insert("trade_partners", "p1", {})
    assert rec.sequence == 1


def test_update_creates_record_with_update_operation():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {"compliance_status": "active"})
    rec = cap.update("trade_partners", "p1", {"compliance_status": "suspended"})
    assert rec.operation == "update"
    assert rec.data["compliance_status"] == "suspended"


def test_delete_creates_record_with_delete_operation():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {"partner_id": "p1"})
    rec = cap.delete("trade_partners", "p1", {"partner_id": "p1"})
    assert rec.operation == "delete"
    assert rec.primary_key == "p1"


def test_invalid_operation_raises_value_error():
    with pytest.raises(ValueError, match="Invalid CDC operation"):
        CDCRecord(operation="invalid_op", table="trade_partners", primary_key="p1", data={})


def test_sequences_are_strictly_increasing():
    cap = CDCCapture()
    r1 = cap.insert("trade_partners", "p1", {})
    r2 = cap.insert("trade_partners", "p2", {})
    r3 = cap.update("trade_partners", "p1", {})
    assert r1.sequence < r2.sequence < r3.sequence


def test_latest_sequence_reflects_last_record():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {})
    cap.insert("trade_partners", "p2", {})
    last = cap.update("trade_partners", "p1", {})
    assert cap.latest_sequence == last.sequence


def test_records_since_returns_only_records_after_offset():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {})
    cap.insert("trade_partners", "p2", {})
    checkpoint = cap.latest_sequence
    r3 = cap.insert("trade_partners", "p3", {})

    replayed = cap.records_since(checkpoint)
    assert len(replayed) == 1
    assert replayed[0].sequence == r3.sequence


def test_records_since_zero_returns_all_records():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {})
    cap.insert("trade_partners", "p2", {})
    cap.delete("trade_partners", "p1", {})
    assert len(cap.records_since(0)) == 3


def test_records_since_latest_sequence_returns_empty():
    cap = CDCCapture()
    cap.insert("trade_partners", "p1", {})
    assert cap.records_since(cap.latest_sequence) == []
