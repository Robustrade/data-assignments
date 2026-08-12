"""
Tests for CDC log capture, sequence monotonicity, and offset-based replay.
"""

import pytest

from pipeline.cdc import CDCCapture, CDCRecord


def test_cdc_record_validation() -> None:
    """Verify CDCRecord rejects invalid operation types."""
    with pytest.raises(ValueError, match="Invalid CDC operation"):
        CDCRecord(
            operation="invalid_op",
            table="customers",
            primary_key="cust_1",
            data={},
        )


def test_cdc_capture_sequence_and_log(cdc_stream: CDCCapture) -> None:
    """Verify insert, update, delete emit monotonically increasing sequence numbers."""
    rec1 = cdc_stream.insert("customers", "c1", {"name": "Alice"})
    rec2 = cdc_stream.update("customers", "c1", {"name": "Alice Smith"})
    rec3 = cdc_stream.delete("customers", "c1", {})

    assert rec1.sequence == 1
    assert rec2.sequence == 2
    assert rec3.sequence == 3
    assert cdc_stream.latest_sequence == 3
    assert len(cdc_stream.log) == 3


def test_cdc_checkpoint_replay(cdc_stream: CDCCapture) -> None:
    """Verify records_since(offset) correctly filters unread records after checkpoint."""
    cdc_stream.insert("wallets", "w1", {"balance": 100})
    cdc_stream.insert("wallets", "w2", {"balance": 200})
    cdc_stream.update("wallets", "w1", {"balance": 150})

    # Checkpoint at sequence 1
    replayed = cdc_stream.records_since(offset=1)
    assert len(replayed) == 2
    assert [r.sequence for r in replayed] == [2, 3]

    # Checkpoint at latest sequence 3
    replayed_empty = cdc_stream.records_since(offset=3)
    assert len(replayed_empty) == 0
