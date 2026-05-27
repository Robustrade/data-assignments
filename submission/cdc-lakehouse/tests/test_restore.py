"""Point-in-time restore from lake."""

from datetime import datetime, timezone

import duckdb

from pipeline.cdc_event import CDCEvent
from pipeline.lake_writer import write_events_to_lake
from pipeline.warehouse_apply import (
    apply_events,
    create_warehouse_tables,
    truncate_warehouse_tables,
)


def test_restore_as_of_filters_events(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    from pipeline import config

    config.LAKE_DIR = tmp_path / "lake"
    wh_path = tmp_path / "wh.duckdb"

    t1 = datetime(2026, 5, 26, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 5, 26, 14, 0, tzinfo=timezone.utc)

    events = [
        CDCEvent(
            1,
            "customers",
            "I",
            '{"customer_id": 1}',
            '{"customer_id": 1, "name": "V1", "email": null, "phone": null, "status": "ACTIVE", "created_at": "2026-05-26T10:00:00", "updated_at": "2026-05-26T10:00:00", "is_deleted": 0}',
            t1,
        ),
        CDCEvent(
            2,
            "customers",
            "U",
            '{"customer_id": 1}',
            '{"customer_id": 1, "name": "V2", "email": null, "phone": null, "status": "ACTIVE", "created_at": "2026-05-26T10:00:00", "updated_at": "2026-05-26T14:00:00", "is_deleted": 0}',
            t2,
        ),
    ]
    write_events_to_lake(events, run_id="r1")

    from pipeline.lake_writer import read_all_lake_events

    as_of = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
    filtered = [e for e in read_all_lake_events() if e.event_ts <= as_of]

    conn = duckdb.connect(str(wh_path))
    create_warehouse_tables(conn)
    truncate_warehouse_tables(conn)
    apply_events(conn, filtered)
    name = conn.execute(
        "SELECT name FROM wh_customers WHERE customer_id = 1"
    ).fetchone()[0]
    assert name == "V1"
