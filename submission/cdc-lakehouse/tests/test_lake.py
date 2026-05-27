"""Lake Parquet writer tests."""

from datetime import datetime, timezone

from pipeline.cdc_event import CDCEvent
from pipeline.lake_writer import read_all_lake_events, write_events_to_lake


def test_write_and_read_lake_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    from pipeline import config

    config.LAKE_DIR = tmp_path / "lake"

    events = [
        CDCEvent(
            event_id=1,
            source_table="customers",
            op="I",
            pk_json='{"customer_id": 1}',
            row_json='{"customer_id": 1, "name": "Alice"}',
            event_ts=datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc),
        ),
        CDCEvent(
            event_id=2,
            source_table="orders",
            op="U",
            pk_json='{"order_id": 10}',
            row_json='{"order_id": 10, "order_status": "PAID"}',
            event_ts=datetime(2026, 5, 26, 12, 1, tzinfo=timezone.utc),
        ),
    ]
    paths = write_events_to_lake(events, run_id="testrun")
    assert len(paths) >= 1

    loaded = read_all_lake_events(tmp_path / "lake")
    assert len(loaded) == 2
    assert loaded[0].event_id == 1
    assert loaded[1].op == "U"
