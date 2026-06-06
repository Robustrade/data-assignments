"""
Lake layer — append-only storage for every CDC event.

Every change is written exactly once. The lake is the source of truth for replay and
historical reconstruction; rows are never updated or deleted.

Idempotency: ``sequence`` is the primary key and appends use ``ON CONFLICT DO NOTHING``,
so duplicate or replayed events cannot corrupt the history.

Production analogue: Parquet/Delta files on object storage, partitioned by
``table_name`` and capture date.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import duckdb

from ingestion.cdc import CDCRecord


def create_lake_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lake_cdc_events (
            sequence     INTEGER  PRIMARY KEY,
            operation    VARCHAR  NOT NULL,
            table_name   VARCHAR  NOT NULL,
            primary_key  VARCHAR  NOT NULL,
            data         VARCHAR  NOT NULL,
            captured_at  TIMESTAMP NOT NULL
        )
        """
    )


def append_to_lake(conn: duckdb.DuckDBPyConnection, records: List[CDCRecord]) -> int:
    """
    Append CDC records to the lake idempotently.

    Returns the number of newly written records (duplicates by ``sequence`` are
    silently skipped).
    """
    if not records:
        return 0

    before = _row_count(conn)
    for r in records:
        conn.execute(
            """
            INSERT INTO lake_cdc_events
                (sequence, operation, table_name, primary_key, data, captured_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (sequence) DO NOTHING
            """,
            [
                r.sequence,
                r.operation,
                r.table,
                r.primary_key,
                _serialize(r.image),
                r.captured_at,
            ],
        )
    return _row_count(conn) - before


def read_events(
    conn: duckdb.DuckDBPyConnection, up_to_sequence: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Read lake events in sequence order, optionally up to ``up_to_sequence``."""
    if up_to_sequence is None:
        rows = conn.execute(
            "SELECT sequence, operation, table_name, primary_key, data, captured_at "
            "FROM lake_cdc_events ORDER BY sequence"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT sequence, operation, table_name, primary_key, data, captured_at "
            "FROM lake_cdc_events WHERE sequence <= ? ORDER BY sequence",
            [up_to_sequence],
        ).fetchall()

    return [
        {
            "sequence": row[0],
            "operation": row[1],
            "table_name": row[2],
            "primary_key": row[3],
            "data": json.loads(row[4]),
            "captured_at": row[5],
        }
        for row in rows
    ]


def _row_count(conn: duckdb.DuckDBPyConnection) -> int:
    return conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]


def _serialize(data: Dict[str, Any]) -> str:
    return json.dumps(data, default=_json_default)


def _json_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
