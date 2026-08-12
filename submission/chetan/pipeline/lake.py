"""
Lake layer — append-only storage for every CDC change event.

Every source change is written exactly once to the lake. The lake is the durable
source of truth for point-in-time replay, historical reconstruction, and audit.

Production analogue: Parquet / Delta Lake files on S3 or GCS partitioned by
`table_name` and `captured_at` date. Append-only semantics: no row is ever deleted or mutated.
"""

from __future__ import annotations

import json
from datetime import datetime

import duckdb

from pipeline.cdc import CDCRecord


def create_lake_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create append-only lake_cdc_events table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake_cdc_events (
            sequence     INTEGER   NOT NULL,
            operation    VARCHAR   NOT NULL,
            table_name   VARCHAR   NOT NULL,
            primary_key  VARCHAR   NOT NULL,
            data         VARCHAR   NOT NULL,
            captured_at  TIMESTAMP NOT NULL
        )
    """)


def append_to_lake(conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]) -> int:
    """
    Append CDC records to the lake with sequence-level deduplication.

    Returns the number of new records successfully written.
    """
    if not records:
        return 0

    create_lake_table(conn)

    # Idempotent deduplication: fetch existing sequence numbers in lake
    existing_seqs = {
        row[0]
        for row in conn.execute("SELECT sequence FROM lake_cdc_events").fetchall()
    }

    new_records = [r for r in records if r.sequence not in existing_seqs]
    if not new_records:
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
        for r in new_records
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
