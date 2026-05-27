"""Warehouse apply logic tests."""

from datetime import datetime, timezone

import duckdb

from pipeline.cdc_event import CDCEvent
from pipeline.warehouse_apply import apply_events, create_warehouse_tables


def _event(eid: int, table: str, op: str, pk: int, row: dict) -> CDCEvent:
    import json

    pk_name = {
        "customers": "customer_id",
        "orders": "order_id",
    }[table]
    return CDCEvent(
        event_id=eid,
        source_table=table,
        op=op,
        pk_json=json.dumps({pk_name: pk}),
        row_json=json.dumps(row),
        event_ts=datetime(2026, 5, 26, tzinfo=timezone.utc),
    )


def test_insert_update_delete_lifecycle():
    conn = duckdb.connect(":memory:")
    create_warehouse_tables(conn)

    apply_events(
        conn,
        [
            _event(
                1,
                "customers",
                "I",
                1,
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "a@x.com",
                    "phone": None,
                    "status": "ACTIVE",
                    "created_at": "2026-05-26T10:00:00",
                    "updated_at": "2026-05-26T10:00:00",
                    "is_deleted": 0,
                },
            ),
            _event(
                2,
                "customers",
                "U",
                1,
                {
                    "customer_id": 1,
                    "name": "Alice Updated",
                    "email": "a@x.com",
                    "phone": None,
                    "status": "SUSPENDED",
                    "created_at": "2026-05-26T10:00:00",
                    "updated_at": "2026-05-26T11:00:00",
                    "is_deleted": 0,
                },
            ),
            _event(
                3,
                "customers",
                "D",
                1,
                {"customer_id": 1},
            ),
        ],
    )

    row = conn.execute(
        "SELECT name, status, _deleted, _cdc_event_id FROM wh_customers WHERE customer_id = 1"
    ).fetchone()
    assert row[2] is True
    assert row[3] == 3


def test_idempotent_replay_skips_stale_events():
    conn = duckdb.connect(":memory:")
    create_warehouse_tables(conn)

    insert = _event(
        10,
        "customers",
        "I",
        5,
        {
            "customer_id": 5,
            "name": "Bob",
            "email": None,
            "phone": None,
            "status": "ACTIVE",
            "created_at": "2026-05-26T10:00:00",
            "updated_at": "2026-05-26T10:00:00",
            "is_deleted": 0,
        },
    )
    apply_events(conn, [insert])
    apply_events(conn, [insert])  # duplicate replay

    count = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    assert count == 1
    name = conn.execute(
        "SELECT name FROM wh_customers WHERE customer_id = 5"
    ).fetchone()[0]
    assert name == "Bob"
