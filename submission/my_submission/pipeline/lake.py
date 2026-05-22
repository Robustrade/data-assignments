"""
    Lake layer — append-only, immutable storage for every CDC event.

    Every change is written exactly once. The lake is the source of truth for:
    - point-in-time replay and recovery
    - historical reconstruction
    - audit trail

    Production analogue: Parquet/Delta files on S3/GCS, partitioned by table_name
    and captured_at date. No row is ever modified or deleted. The immutability
    guarantee enables strong consistency and reproducibility.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import duckdb

from pipeline.cdc import CDCRecord


def create_lake_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the immutable lake table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake_cdc_events (
            sequence     INTEGER  PRIMARY KEY,
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
        
        Idempotency: sequence is the CDC offset/LSN analogue, so retries skip
        events already present in the lake.
    """
    if not records:
        return 0

    existing = {
        row[0]
        for row in conn.execute(
            "SELECT sequence FROM lake_cdc_events WHERE sequence IN "
            f"({','.join(['?'] * len(records))})",
            [r.sequence for r in records],
        ).fetchall()
    }

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
        if r.sequence not in existing
    ]
    if not rows:
        return 0
    conn.executemany(
        "INSERT INTO lake_cdc_events VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def _serialize(data: dict) -> str:
    """Serialize a data dict to JSON, handling special types."""
    return json.dumps(data, default=_json_default)


def _json_default(obj: object) -> str:
    """Custom JSON serializer for non-standard types."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
