"""
Lake layer — append-only history of every CDC event.

Design contract
---------------
The lake is the system of record for **change history**. Every CDC
record is written exactly once; rows are never updated or deleted. If
the warehouse loses state, we can rebuild it deterministically by
replaying `lake_cdc_events` ordered by `sequence`.

Production analogue
-------------------
Parquet (or Delta / Iceberg) files on object storage, partitioned by
`table_name` and a `captured_at` date. The PK on `sequence` here plays
the role of the Kafka offset in the lake's `_kafka_offset` column.

Idempotency
-----------
If the same event is appended twice (after a retried batch, say), the
PK on `sequence` rejects the duplicate at the database level rather
than silently double-writing. The orchestrator catches this and
continues — see `pipeline.orchestrator.replay_to_lake`.

Schema
------
  sequence       INTEGER NOT NULL PRIMARY KEY  -- monotonic capture offset
  operation      VARCHAR  NOT NULL             -- 'insert' | 'update' | 'delete'
  table_name     VARCHAR  NOT NULL
  primary_key    VARCHAR  NOT NULL
  before_image   VARCHAR  (JSON, nullable)     -- null on inserts
  after_image    VARCHAR  (JSON, nullable)     -- null on deletes
  captured_at    TIMESTAMP NOT NULL

We store the row images as JSON strings rather than typed columns
because schema drift would otherwise be a problem inside the lake
itself; the warehouse models do the typed projection.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import duckdb

from pipeline.cdc import CDCRecord


def create_lake_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake_cdc_events (
            sequence      INTEGER  NOT NULL PRIMARY KEY,
            operation     VARCHAR  NOT NULL,
            table_name    VARCHAR  NOT NULL,
            primary_key   VARCHAR  NOT NULL,
            before_image  VARCHAR,
            after_image   VARCHAR,
            captured_at   TIMESTAMP NOT NULL
        )
    """)
    # An index on (table_name, sequence) is what replay queries actually use.
    conn.execute("""
        CREATE INDEX IF NOT EXISTS ix_lake_table_sequence
        ON lake_cdc_events (table_name, sequence)
    """)


def append_to_lake(
    conn: duckdb.DuckDBPyConnection,
    records: list[CDCRecord],
) -> int:
    """
    Append CDC records to the lake. Idempotent by `sequence`: duplicates
    are silently skipped, so retries do not double-write.

    Returns the number of records actually persisted.
    """
    if not records:
        return 0

    written = 0
    for r in records:
        before_json = _serialize(r.before) if r.before is not None else None
        after_json = _serialize(r.after) if r.after is not None else None
        # ON CONFLICT DO NOTHING gives us at-most-once semantics on the lake
        # without making the caller responsible for dedup.
        cursor = conn.execute(
            """
            INSERT INTO lake_cdc_events
                (sequence, operation, table_name, primary_key,
                 before_image, after_image, captured_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (sequence) DO NOTHING
            """,
            [
                r.sequence,
                r.operation,
                r.table,
                r.primary_key,
                before_json,
                after_json,
                r.captured_at,
            ],
        )
        # DuckDB returns rowcount via .fetchall() differently per version;
        # we count by checking whether the row is now present.
        if cursor.execute(
            "SELECT 1 FROM lake_cdc_events WHERE sequence = ?", [r.sequence]
        ).fetchone():
            written += 1
    return written


def replay_records(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str | None = None,
    until_sequence: int | None = None,
) -> list[dict]:
    """
    Read change history back out of the lake in sequence order.

    Used by:
      - the warehouse's "rebuild from scratch" path
      - the time-travel scripts
      - audit queries

    Filters are optional: pass `table` to restrict to one source table,
    `until_sequence` to bound the replay (inclusive).
    """
    sql = """
        SELECT sequence, operation, table_name, primary_key,
               before_image, after_image, captured_at
        FROM   lake_cdc_events
    """
    where: list[str] = []
    params: list[object] = []
    if table is not None:
        where.append("table_name = ?")
        params.append(table)
    if until_sequence is not None:
        where.append("sequence <= ?")
        params.append(until_sequence)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY sequence"
    rows = conn.execute(sql, params).fetchall()

    out: list[dict] = []
    for seq, op, tbl, pk, before_s, after_s, captured in rows:
        out.append({
            "sequence":    seq,
            "operation":   op,
            "table":       tbl,
            "primary_key": pk,
            "before":      json.loads(before_s) if before_s else None,
            "after":       json.loads(after_s) if after_s else None,
            "captured_at": captured,
        })
    return out


# ── JSON helpers ─────────────────────────────────────────────────────────────


def _serialize(data: dict) -> str:
    return json.dumps(data, default=_json_default, sort_keys=True)


def _json_default(obj: object) -> object:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        # Decimals serialize as strings to avoid float-precision drift.
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
