"""
Warehouse layer — current-state snapshot + SCD2 history.

Three tables per source table
-----------------------------
  wh_<table>          : latest known state, soft-deleted with _deleted=true
  wh_<table>_history  : SCD2 — every revision has _valid_from / _valid_to /
                        _is_current / _op so we can answer "what did this
                        row look like at time T?" without rescanning the lake
  (lake_cdc_events keeps the raw events; the SCD2 table is a *materialized
   view of the lake* that downstream consumers can query directly)

Idempotent merge
----------------
Each warehouse row carries `_cdc_seq`, the sequence of the last CDC
event that touched it. The merge applies an event only when its
sequence is **strictly greater** than the row's `_cdc_seq`. This makes
the merge:
  - safe under retry / replay (duplicates are no-ops)
  - safe under out-of-order arrival (older events do not clobber newer
    state)

SCD2 mechanics
--------------
For an insert: open a new row with `_valid_from = captured_at`,
`_valid_to = NULL`, `_is_current = true`, `_op = 'insert'`.

For an update or delete on an existing row:
  1. close the current row by setting `_valid_to = captured_at`,
     `_is_current = false`
  2. for updates, open a new current row with the post-image
  3. for deletes, do not open a new row but mark the *current* row's
     replacement (none) as deleted in `wh_<table>` via `_deleted=true`

Time travel
-----------
Use `state_at(conn, table, ts)` to reconstruct what the table looked
like at any timestamp. The query selects rows from `wh_<table>_history`
where `_valid_from <= ts < COALESCE(_valid_to, '9999-12-31')` and
`_op != 'delete'`.

Production analogue
-------------------
The warehouse is what BigQuery / Snowflake / Redshift would expose:
typed, queryable, current state for analytics, with the SCD2 history
table for finance / audit / restore. The merge here is the same
MERGE-by-PK pattern you would write in those systems, just expressed
in DuckDB SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable

import duckdb

from pipeline.cdc import CDCRecord
from source.contracts import CONTRACTS


# Source tables we mirror in the warehouse, in dependency order.
WAREHOUSED_TABLES: tuple[str, ...] = (
    "customers",
    "payment_methods",
    "wallets",
    "transfers",
    "transfer_events",
    "ledger_entries",
)


# Map source SQL types to warehouse SQL types. Identical for our domain;
# the indirection lets us coerce on the way in if a source type is wider
# than what we want to expose to BI.
_TYPE_MAP: dict[str, str] = {
    "VARCHAR":       "VARCHAR",
    "BOOLEAN":       "BOOLEAN",
    "DATE":          "DATE",
    "TIMESTAMP":     "TIMESTAMP",
    "DECIMAL(18,4)": "DECIMAL(18,4)",
}


@dataclass
class _ColumnInfo:
    name: str
    sql_type: str
    nullable: bool


def _columns_for(table: str) -> list[_ColumnInfo]:
    contract = CONTRACTS[table]
    return [
        _ColumnInfo(c.name, _TYPE_MAP[c.type], c.nullable) for c in contract.columns
    ]


def _wh_table(table: str) -> str:
    return f"wh_{table}"


def _hist_table(table: str) -> str:
    return f"wh_{table}_history"


# ── DDL ──────────────────────────────────────────────────────────────────────


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for table in WAREHOUSED_TABLES:
        cols = _columns_for(table)
        pk = CONTRACTS[table].primary_key

        # Current-state table.
        col_defs = ", ".join(
            f"{c.name} {c.sql_type}{'' if c.nullable else ' NOT NULL'}"
            if c.name == pk
            else f"{c.name} {c.sql_type}"
            for c in cols
        )
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_wh_table(table)} (
                {col_defs},
                _cdc_seq   INTEGER NOT NULL,
                _deleted   BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY ({pk})
            )
        """)

        # SCD2 history table — same columns, no PK on business key,
        # surrogate PK on (business_key, _valid_from).
        hist_cols = ", ".join(f"{c.name} {c.sql_type}" for c in cols)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_hist_table(table)} (
                {hist_cols},
                _cdc_seq     INTEGER   NOT NULL,
                _valid_from  TIMESTAMP NOT NULL,
                _valid_to    TIMESTAMP,
                _is_current  BOOLEAN   NOT NULL,
                _op          VARCHAR   NOT NULL
                             CHECK (_op IN ('insert', 'update', 'delete')),
                PRIMARY KEY ({pk}, _valid_from)
            )
        """)


# ── apply CDC records ────────────────────────────────────────────────────────


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection,
    records: Iterable[CDCRecord],
) -> int:
    """
    Apply CDC records to the current-state and SCD2 history tables.

    Returns the number of records *actually applied* (skipped duplicates
    and out-of-order events are not counted).
    """
    sorted_records = sorted(records, key=lambda r: r.sequence)
    applied = 0
    for r in sorted_records:
        if r.table not in WAREHOUSED_TABLES:
            continue
        if _apply_one(conn, r):
            applied += 1
    return applied


def _apply_one(conn: duckdb.DuckDBPyConnection, r: CDCRecord) -> bool:
    pk_col = CONTRACTS[r.table].primary_key
    cur = _wh_table(r.table)
    hist = _hist_table(r.table)

    existing = conn.execute(
        f"SELECT _cdc_seq, _deleted FROM {cur} WHERE {pk_col} = ?",
        [r.primary_key],
    ).fetchone()

    # Idempotent / out-of-order guard.
    if existing is not None and existing[0] >= r.sequence:
        return False

    # Close any open SCD2 row for this PK.
    conn.execute(
        f"""
        UPDATE {hist}
        SET    _valid_to    = ?,
               _is_current  = FALSE
        WHERE  {pk_col}     = ?
          AND  _is_current  = TRUE
        """,
        [r.captured_at, r.primary_key],
    )

    if r.operation == "delete":
        # Update current-state to mark deleted.
        if existing is not None:
            conn.execute(
                f"UPDATE {cur} SET _deleted = TRUE, _cdc_seq = ? "
                f"WHERE {pk_col} = ?",
                [r.sequence, r.primary_key],
            )
        # Append a closed delete row to history (with the pre-image).
        _insert_history(
            conn,
            r.table,
            r,
            data=r.before or {},
            valid_from=r.captured_at,
            valid_to=r.captured_at,
            is_current=False,
            op="delete",
        )
        return True

    # insert or update
    data = r.after or {}
    cols = _columns_for(r.table)
    values = [data.get(c.name) for c in cols]

    if existing is None:
        # INSERT into current-state.
        col_list = ", ".join(c.name for c in cols)
        placeholders = ", ".join(["?"] * len(cols))
        conn.execute(
            f"""
            INSERT INTO {cur} ({col_list}, _cdc_seq, _deleted)
            VALUES ({placeholders}, ?, FALSE)
            """,
            values + [r.sequence],
        )
    else:
        # UPDATE the current-state row.
        set_clause = ", ".join(f"{c.name} = ?" for c in cols)
        conn.execute(
            f"""
            UPDATE {cur}
            SET    {set_clause}, _cdc_seq = ?, _deleted = FALSE
            WHERE  {pk_col}     = ?
            """,
            values + [r.sequence, r.primary_key],
        )

    # Open new current SCD2 row.
    _insert_history(
        conn,
        r.table,
        r,
        data=data,
        valid_from=r.captured_at,
        valid_to=None,
        is_current=True,
        op=r.operation,
    )
    return True


def _insert_history(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    record: CDCRecord,
    *,
    data: dict[str, Any],
    valid_from: datetime,
    valid_to: datetime | None,
    is_current: bool,
    op: str,
) -> None:
    cols = _columns_for(table)
    values = [data.get(c.name) for c in cols]
    col_list = ", ".join(c.name for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    conn.execute(
        f"""
        INSERT INTO {_hist_table(table)}
            ({col_list}, _cdc_seq, _valid_from, _valid_to, _is_current, _op)
        VALUES ({placeholders}, ?, ?, ?, ?, ?)
        ON CONFLICT ({CONTRACTS[table].primary_key}, _valid_from) DO NOTHING
        """,
        values + [record.sequence, valid_from, valid_to, is_current, op],
    )


# ── time travel ──────────────────────────────────────────────────────────────


def state_at(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    ts: datetime,
) -> list[dict]:
    """
    Reconstruct the state of `table` at timestamp `ts` from the SCD2 history.

    Returns the rows that were *valid at ts* and not marked deleted at that
    point — semantically equivalent to running `SELECT * FROM <table>` on
    the source as of `ts`.
    """
    cols = _columns_for(table)
    col_list = ", ".join(c.name for c in cols)
    sql = f"""
        SELECT {col_list}
        FROM   {_hist_table(table)}
        WHERE  _valid_from <= ?
          AND  COALESCE(_valid_to, TIMESTAMP '9999-12-31 23:59:59') > ?
          AND  _op <> 'delete'
    """
    rows = conn.execute(sql, [ts, ts]).fetchall()
    return [
        {c.name: _coerce(value) for c, value in zip(cols, row)}
        for row in rows
    ]


def _coerce(v: object) -> object:
    """Bring DuckDB result values back to standard Python types for callers."""
    if isinstance(v, Decimal):
        return v
    return v


# ── restore ──────────────────────────────────────────────────────────────────


def restore_current_state_to(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    ts: datetime,
) -> int:
    """
    Replace `wh_<table>` rows with the snapshot from `state_at(table, ts)`.

    This is the operational hook for "roll the warehouse back to time T":
    history is preserved (we never touch `wh_<table>_history`), but the
    *current state* table is overwritten from the historical snapshot.

    Returns the number of rows in the restored snapshot.
    """
    snapshot = state_at(conn, table, ts)
    pk_col = CONTRACTS[table].primary_key
    cols = _columns_for(table)

    conn.execute(f"DELETE FROM {_wh_table(table)}")

    if not snapshot:
        return 0

    col_list = ", ".join(c.name for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    for row in snapshot:
        values = [row.get(c.name) for c in cols]
        conn.execute(
            f"""
            INSERT INTO {_wh_table(table)}
                ({col_list}, _cdc_seq, _deleted)
            VALUES ({placeholders}, 0, FALSE)
            """,
            values,
        )
    # Note: _cdc_seq is reset to 0 to signal "restored, ready for new events".
    # The next CDC record (which will have sequence > 0) will reapply cleanly.
    _ = pk_col  # quiet linter — we may use it for a future assertion
    return len(snapshot)
