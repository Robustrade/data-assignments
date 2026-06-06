"""CDC correctness tests: capture, replay, restart, duplicates."""

import pytest

from ingestion.cdc import CDCCapture, CDCRecord


def test_invalid_operation_rejected():
    with pytest.raises(ValueError):
        CDCRecord("upsert", "customers", "c1", None, {})


def test_insert_update_delete_captured():
    cap = CDCCapture()
    cap.insert("customers", "c1", {"customer_id": "c1"})
    cap.update("customers", "c1", {"customer_id": "c1", "name": "new"})
    cap.delete("customers", "c1", before={"customer_id": "c1"})

    ops = [r.operation for r in cap.log]
    assert ops == ["insert", "update", "delete"]
    assert [r.sequence for r in cap.log] == [1, 2, 3]


def test_before_after_images():
    cap = CDCCapture()
    rec = cap.update("orders", "o1", {"status": "paid"}, before={"status": "pending"})
    assert rec.before == {"status": "pending"}
    assert rec.after == {"status": "paid"}
    assert rec.image == {"status": "paid"}

    dele = cap.delete("orders", "o1", before={"status": "paid"})
    assert dele.after is None
    assert dele.image == {"status": "paid"}  # delete image falls back to before


def test_records_since_checkpoint_replay():
    cap = CDCCapture()
    for i in range(5):
        cap.insert("customers", f"c{i}", {"customer_id": f"c{i}"})

    tail = cap.records_since(3)
    assert [r.sequence for r in tail] == [4, 5]
    assert cap.records_since(cap.latest_sequence) == []


def test_restart_resumes_from_checkpoint():
    cap = CDCCapture()
    cap.insert("customers", "c1", {"customer_id": "c1"})
    cap.insert("customers", "c2", {"customer_id": "c2"})

    checkpoint = cap.latest_sequence
    cap.insert("customers", "c3", {"customer_id": "c3"})

    new = cap.records_since(checkpoint)
    assert [r.primary_key for r in new] == ["c3"]
