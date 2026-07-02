from __future__ import annotations

import json
from datetime import datetime, timezone

from pipeline.cdc import CDCEvent, KafkaCDCLog
from pipeline.lake import append_events
from pipeline.warehouse import apply_events, point_in_time_state


def test_lake_keeps_every_change_and_deduplicates_replay(conn, reservation_row):
    log = KafkaCDCLog()
    created = log.create("reservations", "r1", reservation_row)
    updated = log.update(
        "reservations",
        "r1",
        reservation_row,
        {**reservation_row, "status": "confirmed"},
    )

    assert append_events(conn, [created, updated]) == 2
    assert append_events(conn, [created, updated]) == 0

    count = conn.execute("SELECT COUNT(*) FROM silver_ticketing_cdc_events").fetchone()[0]
    assert count == 2


def test_warehouse_current_uses_latest_lsn(conn, reservation_row):
    log = KafkaCDCLog()
    created = log.create("reservations", "r1", reservation_row)
    updated = log.update(
        "reservations",
        "r1",
        reservation_row,
        {**reservation_row, "status": "confirmed"},
    )

    apply_events(conn, [updated, created])

    row_json, is_deleted, source_lsn = conn.execute(
        """
        SELECT row_json, is_deleted, source_lsn
        FROM wh_current_records
        WHERE source_table = 'reservations' AND primary_key = 'r1'
        """
    ).fetchone()

    assert json.loads(row_json)["status"] == "confirmed"
    assert is_deleted is False
    assert source_lsn == updated.source_lsn


def test_delete_becomes_soft_delete_in_warehouse(conn, reservation_row):
    log = KafkaCDCLog()
    created = log.create("reservations", "r1", reservation_row)
    deleted = log.delete("reservations", "r1", reservation_row)

    apply_events(conn, [created, deleted])

    assert conn.execute(
        "SELECT is_deleted FROM wh_current_records WHERE primary_key = 'r1'"
    ).fetchone()[0] is True


def test_history_supports_point_in_time_restore(conn, reservation_row):
    held = CDCEvent(
        topic="ticketing.public.reservations",
        table="reservations",
        primary_key="r1",
        op="c",
        after=reservation_row,
        source_lsn=1,
        tx_id="tx-1",
        kafka_offset=1,
        source_event_at=datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
    )
    confirmed = CDCEvent(
        topic="ticketing.public.reservations",
        table="reservations",
        primary_key="r1",
        op="u",
        before=reservation_row,
        after={**reservation_row, "status": "confirmed"},
        source_lsn=2,
        tx_id="tx-2",
        kafka_offset=2,
        source_event_at=datetime(2026, 7, 1, 10, 5, tzinfo=timezone.utc),
    )

    apply_events(conn, [held, confirmed])

    restored = point_in_time_state(conn, "reservations", "2026-07-01 10:02:00")
    assert len(restored) == 1
    assert json.loads(restored[0][1])["status"] == "held"
