"""Snowflake-like current and history warehouse models."""

from __future__ import annotations

import json

import duckdb

from pipeline.cdc import CDCEvent


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_current_records (
            source_table         VARCHAR NOT NULL,
            primary_key          VARCHAR NOT NULL,
            row_json             VARCHAR NOT NULL,
            is_deleted           BOOLEAN NOT NULL,
            source_lsn           BIGINT NOT NULL,
            source_event_at      TIMESTAMP NOT NULL,
            warehouse_updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (source_table, primary_key)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_history_records (
            event_id        VARCHAR PRIMARY KEY,
            source_table    VARCHAR NOT NULL,
            primary_key     VARCHAR NOT NULL,
            row_json        VARCHAR NOT NULL,
            valid_from      TIMESTAMP NOT NULL,
            valid_to        TIMESTAMP,
            is_current      BOOLEAN NOT NULL,
            is_deleted      BOOLEAN NOT NULL,
            source_lsn      BIGINT NOT NULL
        )
    """)


def apply_events(conn: duckdb.DuckDBPyConnection, events: list[CDCEvent]) -> None:
    for event in sorted(events, key=lambda item: (item.source_lsn, item.kafka_offset)):
        _upsert_current(conn, event)
        _append_history(conn, event)


def point_in_time_state(
    conn: duckdb.DuckDBPyConnection, table: str, as_of: str
) -> list[tuple[str, str]]:
    return conn.execute(
        """
        SELECT primary_key, row_json
        FROM wh_history_records
        WHERE source_table = ?
          AND valid_from <= CAST(? AS TIMESTAMP)
          AND (valid_to IS NULL OR valid_to > CAST(? AS TIMESTAMP))
          AND is_deleted = false
        ORDER BY primary_key
        """,
        [table, as_of, as_of],
    ).fetchall()


def _upsert_current(conn: duckdb.DuckDBPyConnection, event: CDCEvent) -> None:
    current = conn.execute(
        """
        SELECT source_lsn
        FROM wh_current_records
        WHERE source_table = ? AND primary_key = ?
        """,
        [event.table, event.primary_key],
    ).fetchone()

    if current and current[0] >= event.source_lsn:
        return

    row_json = json.dumps(event.current_image, sort_keys=True, default=str)
    is_deleted = event.op == "d"

    if current:
        conn.execute(
            """
            UPDATE wh_current_records
            SET row_json = ?,
                is_deleted = ?,
                source_lsn = ?,
                source_event_at = ?,
                warehouse_updated_at = CURRENT_TIMESTAMP
            WHERE source_table = ? AND primary_key = ?
            """,
            [
                row_json,
                is_deleted,
                event.source_lsn,
                event.source_event_at,
                event.table,
                event.primary_key,
            ],
        )
    else:
        conn.execute(
            """
            INSERT INTO wh_current_records
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                event.table,
                event.primary_key,
                row_json,
                is_deleted,
                event.source_lsn,
                event.source_event_at,
            ],
        )


def _append_history(conn: duckdb.DuckDBPyConnection, event: CDCEvent) -> None:
    exists = conn.execute(
        "SELECT COUNT(*) FROM wh_history_records WHERE event_id = ?",
        [event.event_id],
    ).fetchone()[0]
    if exists:
        return

    conn.execute(
        """
        UPDATE wh_history_records
        SET valid_to = ?, is_current = false
        WHERE source_table = ?
          AND primary_key = ?
          AND is_current = true
          AND source_lsn < ?
        """,
        [event.source_event_at, event.table, event.primary_key, event.source_lsn],
    )

    conn.execute(
        """
        INSERT INTO wh_history_records
        VALUES (?, ?, ?, ?, ?, NULL, true, ?, ?)
        """,
        [
            event.event_id,
            event.table,
            event.primary_key,
            json.dumps(event.current_image, sort_keys=True, default=str),
            event.source_event_at,
            event.op == "d",
            event.source_lsn,
        ],
    )
