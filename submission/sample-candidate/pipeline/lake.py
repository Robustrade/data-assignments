"""
Lake layer — append-only storage for every CDC event.

Every change is written exactly once. The lake is the source of truth for
point-in-time replay and historical reconstruction.

Production analogue: Parquet/Delta files on S3 or GCS, partitioned by
table_name and captured_at date. No row is ever modified or deleted.
"""

from __future__ import annotations

import json
from datetime import datetime

import duckdb

from pipeline.cdc import CDCRecord


def create_lake_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake_cdc_events (
            sequence     INTEGER  NOT NULL,
            operation    VARCHAR  NOT NULL,
            table_name   VARCHAR  NOT NULL,
            primary_key  VARCHAR  NOT NULL,
            data         VARCHAR  NOT NULL,
            captured_at  TIMESTAMP NOT NULL
        )
    """)


def append_to_lake(conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]) -> int:
    """
    Append CDC records to the lake.

    Returns the number of records written.
    Idempotency note: in production, deduplicate by sequence before appending.
    """
    if not records:
        return 0

    rows = [
        (
            r.sequence,
            r.operation,
            r.table,
            r.primary_key,
            _serialize(r.data),
            r.captured_at,
        )
        for r in records
    ]
    conn.executemany(
        "INSERT INTO lake_cdc_events VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def _serialize(data: dict) -> str:
    return json.dumps(data, default=_json_default)


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
