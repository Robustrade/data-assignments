"""CDC event model tests."""

from datetime import datetime, timezone

import pytest

from pipeline.cdc_event import CDCEvent


def test_valid_ops():
    CDCEvent(
        event_id=1,
        source_table="customers",
        op="I",
        pk_json='{"customer_id": 1}',
        row_json='{"customer_id": 1}',
        event_ts=datetime.now(timezone.utc),
    )


def test_invalid_op_raises():
    with pytest.raises(ValueError, match="Invalid op"):
        CDCEvent(
            event_id=1,
            source_table="customers",
            op="X",
            pk_json="{}",
            row_json="{}",
            event_ts=datetime.now(timezone.utc),
        )
