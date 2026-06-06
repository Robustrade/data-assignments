"""
Warehouse layer — current-state snapshot + SCD2 history built from CDC events.

For every source table the warehouse keeps two coordinated models:

1. ``wh_<table>``         current snapshot (latest state, soft-delete via ``_deleted``)
2. ``wh_<table>_history`` SCD2 versions (``_valid_from_seq`` / ``_valid_to_seq`` /
                          ``_is_current``) enabling point-in-time reconstruction

Both are derived from the same sequence-ordered change stream, so the snapshot can
never silently drift from history.

Restore / time travel:
- ``reconstruct_as_of`` returns table state at a given sequence from the SCD2 history.
- ``restore_from_lake`` rebuilds the current snapshot deterministically by replaying
  the durable lake up to a target sequence.

Production analogue: BigQuery/Snowflake current-state tables refreshed by a streaming
merge, with SCD2 history tables alongside for temporal queries.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import duckdb

from ingestion.cdc import CDCRecord
from lake.lake import read_events
from source.models import SCHEMA_CONTRACT


def current_table(source_table: str) -> str:
    return f"wh_{source_table}"


def history_table(source_table: str) -> str:
    return f"wh_{source_table}_history"


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create current + SCD2 history tables for every contracted source table."""
    for table, contract in SCHEMA_CONTRACT.items():
        col_defs = ",\n            ".join(
            f"{c.name} {c.dtype}" for c in contract.columns
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {current_table(table)} (
            {col_defs},
            _cdc_seq INTEGER NOT NULL,
            _deleted BOOLEAN NOT NULL DEFAULT false,
            PRIMARY KEY ({contract.primary_key})
            )
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {history_table(table)} (
            {col_defs},
            _valid_from_seq INTEGER NOT NULL,
            _valid_to_seq   INTEGER,
            _is_current     BOOLEAN NOT NULL DEFAULT true,
            _operation      VARCHAR NOT NULL,
            valid_from      TIMESTAMP NOT NULL,
            valid_to        TIMESTAMP,
            PRIMARY KEY ({contract.primary_key}, _valid_from_seq)
            )
            """
        )


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection, records: List[CDCRecord]
) -> None:
    """
    Apply CDC records (in sequence order) to current snapshot + SCD2 history.

    Idempotent / out-of-order safe: a record is skipped if the row's current
    ``_cdc_seq`` is already >= this record's sequence.
    """
    for record in sorted(records, key=lambda r: r.sequence):
        contract = SCHEMA_CONTRACT.get(record.table)
        if contract is None:
            continue

        pk_col = contract.primary_key
        pk_val = record.primary_key
        existing_seq = _current_seq(conn, record.table, pk_col, pk_val)
        if existing_seq is not None and existing_seq >= record.sequence:
            continue  # duplicate or stale event

        captured_at = record.captured_at
        if record.operation == "delete":
            _close_open_version(
                conn, record.table, pk_col, pk_val, record.sequence, captured_at
            )
            _soft_delete_current(conn, record.table, pk_col, pk_val, record.sequence)
        else:
            data = _filter_columns(contract, record.image)
            _close_open_version(
                conn, record.table, pk_col, pk_val, record.sequence, captured_at
            )
            _open_history_version(
                conn,
                record.table,
                contract,
                data,
                record.operation,
                record.sequence,
                captured_at,
            )
            _upsert_current(conn, record.table, contract, data, record.sequence)


# -- reconstruction / restore -------------------------------------------------


def reconstruct_as_of(
    conn: duckdb.DuckDBPyConnection, source_table: str, as_of_seq: int
) -> List[Dict[str, Any]]:
    """Return rows of ``source_table`` as they existed at ``as_of_seq`` (time travel)."""
    contract = SCHEMA_CONTRACT[source_table]
    cols = ", ".join(contract.column_names)
    rows = conn.execute(
        f"""
        SELECT {cols}
        FROM {history_table(source_table)}
        WHERE _valid_from_seq <= ?
          AND (_valid_to_seq IS NULL OR _valid_to_seq > ?)
        ORDER BY {contract.primary_key}
        """,
        [as_of_seq, as_of_seq],
    ).fetchall()
    return [dict(zip(contract.column_names, row)) for row in rows]


def restore_from_lake(
    conn: duckdb.DuckDBPyConnection, target_seq: Optional[int] = None
) -> None:
    """
    Rebuild the current snapshot from the durable lake, replaying up to ``target_seq``.

    This is the operational rollback path: it proves the warehouse can be reconstructed
    purely from lake history.
    """
    for table in SCHEMA_CONTRACT:
        conn.execute(f"DELETE FROM {current_table(table)}")

    for event in read_events(conn, target_seq):
        contract = SCHEMA_CONTRACT.get(event["table_name"])
        if contract is None:
            continue
        pk_col = contract.primary_key
        pk_val = event["primary_key"]
        seq = event["sequence"]
        if event["operation"] == "delete":
            existing = _current_seq(conn, event["table_name"], pk_col, pk_val)
            if existing is not None:
                _soft_delete_current(conn, event["table_name"], pk_col, pk_val, seq)
        else:
            data = _filter_columns(contract, event["data"])
            _upsert_current(conn, event["table_name"], contract, data, seq)


# -- internal helpers ---------------------------------------------------------


def _current_seq(
    conn: duckdb.DuckDBPyConnection, table: str, pk_col: str, pk_val: str
) -> Optional[int]:
    row = conn.execute(
        f"SELECT _cdc_seq FROM {current_table(table)} WHERE {pk_col} = ?",
        [pk_val],
    ).fetchone()
    return int(row[0]) if row else None


def _filter_columns(contract, image: Dict[str, Any]) -> Dict[str, Any]:
    return {c.name: image.get(c.name) for c in contract.columns if c.name in image}


def _upsert_current(conn, table, contract, data: Dict[str, Any], seq: int) -> None:
    cols = list(data.keys()) + ["_cdc_seq", "_deleted"]
    vals = list(data.values()) + [seq, False]
    placeholders = ", ".join(["?"] * len(vals))
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols)
    conn.execute(
        f"""
        INSERT INTO {current_table(table)} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({contract.primary_key}) DO UPDATE SET {updates}
        """,
        vals,
    )


def _soft_delete_current(conn, table, pk_col, pk_val, seq: int) -> None:
    conn.execute(
        f"UPDATE {current_table(table)} SET _deleted = true, _cdc_seq = ? "
        f"WHERE {pk_col} = ?",
        [seq, pk_val],
    )


def _close_open_version(conn, table, pk_col, pk_val, seq: int, captured_at) -> None:
    conn.execute(
        f"""
        UPDATE {history_table(table)}
        SET _valid_to_seq = ?, valid_to = ?, _is_current = false
        WHERE {pk_col} = ? AND _is_current = true
        """,
        [seq, captured_at, pk_val],
    )


def _open_history_version(
    conn, table, contract, data: Dict[str, Any], operation: str, seq: int, captured_at
) -> None:
    cols = list(data.keys()) + [
        "_valid_from_seq",
        "_valid_to_seq",
        "_is_current",
        "_operation",
        "valid_from",
        "valid_to",
    ]
    vals = list(data.values()) + [seq, None, True, operation, captured_at, None]
    placeholders = ", ".join(["?"] * len(vals))
    conn.execute(
        f"""
        INSERT INTO {history_table(table)} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({contract.primary_key}, _valid_from_seq) DO NOTHING
        """,
        vals,
    )
