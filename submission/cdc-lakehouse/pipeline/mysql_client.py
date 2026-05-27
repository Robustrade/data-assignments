"""MySQL connectivity for CDC ingestion."""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from pipeline.config import MYSQL_CONFIG
from pipeline.cdc_event import CDCEvent


def connect_mysql() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        cursorclass=DictCursor,
        autocommit=True,
    )


def fetch_cdc_events_since(
    conn: pymysql.connections.Connection, last_event_id: int
) -> list[CDCEvent]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, source_table, op, pk_json, row_json, event_ts
            FROM cdc_events
            WHERE event_id > %s
            ORDER BY event_id ASC
            """,
            (last_event_id,),
        )
        rows = cur.fetchall()
    return [CDCEvent.from_mysql_row(row) for row in rows]


def fetch_information_schema(
    conn: pymysql.connections.Connection,
) -> list[dict[str, Any]]:
    tables = (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "order_status_events",
    )
    placeholders = ", ".join(["%s"] * len(tables))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT table_name, column_name, data_type, is_nullable, column_key
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name IN ({placeholders})
            ORDER BY table_name, ordinal_position
            """,
            [MYSQL_CONFIG["database"], *tables],
        )
        return list(cur.fetchall())
