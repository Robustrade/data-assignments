from __future__ import annotations

import pytest

from pipeline.cdc import CDCEvent, KafkaCDCLog


def test_create_update_delete_events_have_debezium_shape(reservation_row):
    log = KafkaCDCLog()
    created = log.create("reservations", "r1", reservation_row)
    updated = {**reservation_row, "status": "confirmed"}
    changed = log.update("reservations", "r1", reservation_row, updated)
    deleted = log.delete("reservations", "r1", updated)

    assert created.op == "c"
    assert changed.before["status"] == "held"
    assert changed.after["status"] == "confirmed"
    assert deleted.op == "d"
    assert deleted.before["reservation_id"] == "r1"


def test_offsets_and_lsns_are_monotonic(reservation_row):
    log = KafkaCDCLog()
    first = log.create("reservations", "r1", reservation_row)
    second = log.update("reservations", "r1", reservation_row, reservation_row)

    assert first.kafka_offset < second.kafka_offset
    assert first.source_lsn < second.source_lsn


def test_events_after_models_checkpoint_replay(reservation_row):
    log = KafkaCDCLog()
    log.create("reservations", "r1", reservation_row)
    checkpoint = log.latest_offset
    event = log.create("reservations", "r2", {**reservation_row, "reservation_id": "r2"})

    assert log.events_after(checkpoint) == [event]


def test_event_id_is_stable_for_duplicate_delivery(reservation_row):
    event = CDCEvent(
        topic="ticketing.public.reservations",
        table="reservations",
        primary_key="r1",
        op="c",
        after=reservation_row,
        source_lsn=100,
        tx_id="tx-100",
    )
    duplicate = CDCEvent(
        topic="ticketing.public.reservations",
        table="reservations",
        primary_key="r1",
        op="c",
        after=reservation_row,
        source_lsn=100,
        tx_id="tx-100",
    )

    assert event.event_id == duplicate.event_id


def test_invalid_operation_is_rejected(reservation_row):
    with pytest.raises(ValueError, match="Invalid Debezium operation"):
        CDCEvent(
            topic="ticketing.public.reservations",
            table="reservations",
            primary_key="r1",
            op="upsert",
            after=reservation_row,
        )

