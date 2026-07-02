"""Silver Iceberg-like append-only storage for every CDC event."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import duckdb

from pipeline.cdc import CDCEvent


def create_lake_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_ticketing_cdc_events (
            event_id        VARCHAR PRIMARY KEY,
            source_table    VARCHAR NOT NULL,
            primary_key     VARCHAR NOT NULL,
            op              VARCHAR NOT NULL,
            before_json     VARCHAR,
            after_json      VARCHAR,
            schema_id       VARCHAR NOT NULL,
            source_lsn      BIGINT NOT NULL,
            tx_id           VARCHAR NOT NULL,
            kafka_topic     VARCHAR NOT NULL,
            kafka_partition INTEGER NOT NULL,
            kafka_offset    BIGINT NOT NULL,
            source_event_at TIMESTAMP NOT NULL,
            ingested_at     TIMESTAMP NOT NULL
        )
    """)


def append_events(conn: duckdb.DuckDBPyConnection, events: list[CDCEvent]) -> int:
    """Append events idempotently, as an Iceberg MERGE would do in production."""
    if not events:
        return 0

    written = 0
    for event in events:
        exists = conn.execute(
            "SELECT COUNT(*) FROM silver_ticketing_cdc_events WHERE event_id = ?",
            [event.event_id],
        ).fetchone()[0]
        if exists:
            continue

        conn.execute(
            """
            INSERT INTO silver_ticketing_cdc_events
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event.event_id,
                event.table,
                event.primary_key,
                event.op,
                _json(event.before),
                _json(event.after),
                event.schema_id,
                event.source_lsn,
                event.tx_id,
                event.topic,
                event.kafka_partition,
                event.kafka_offset,
                event.source_event_at,
                datetime.utcnow(),
            ],
        )
        written += 1

    return written


def _json(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)
