#!/usr/bin/env python3
"""
cdc_extract.py — polling-based CDC extractor.

For each source table, pulls all rows with updated_at > last checkpoint
(using (updated_at, id) as a stable cursor to avoid skipping same-timestamp
rows), appends them as change records to the lake (JSON lines, one file per
table), and only then advances the checkpoint in Postgres.

Idempotency: each lake record is keyed by (table, id, updated_at). Re-running
against the same window re-writes the same records rather than duplicating
downstream state — the lake file is append-only, but downstream consumers
(warehouse loader) de-dupe on that key.

Usage:
    python3 cdc_extract.py
"""

import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

DB_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="wallet_db",
    user="postgres",
    password="postgres",
)

LAKE_DIR = "lake"
TABLES = [
    "customer",
    "wallet",
    "transfer",
    "wallet_balance_history",
    "payment_attempt",
]


def get_checkpoint(cur, table):
    cur.execute(
        "SELECT last_updated_at, last_id FROM cdc_checkpoint WHERE table_name = %s",
        (table,),
    )
    row = cur.fetchone()
    return row["last_updated_at"], row["last_id"]


def advance_checkpoint(cur, table, last_updated_at, last_id):
    cur.execute(
        """
        UPDATE cdc_checkpoint
        SET last_updated_at = %s, last_id = %s
        WHERE table_name = %s
        """,
        (last_updated_at, last_id, table),
    )


def extract_table(conn, table):
    """
    Returns the list of changed rows (as dicts) for this table since the
    last checkpoint, and the new (updated_at, id) cursor to advance to.
    Uses a compound cursor (updated_at, id) so multiple rows sharing the
    same updated_at timestamp aren't skipped on the next run.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        last_updated_at, last_id = get_checkpoint(cur, table)

        cur.execute(
            f"""
            SELECT * FROM {table}
            WHERE (updated_at, id) > (%s, %s)
            ORDER BY updated_at ASC, id ASC
            """,
            (last_updated_at, last_id),
        )
        rows = cur.fetchall()

        if not rows:
            return [], None

        new_cursor = (rows[-1]["updated_at"], rows[-1]["id"])
        return rows, new_cursor


def to_change_record(table, row):
    op = "delete" if row.get("is_deleted") else "upsert"
    # Serialize non-JSON-native types (datetime, Decimal) to strings.
    payload = {
        k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
        for k, v in row.items()
    }
    return {
        "table": table,
        "op": op,
        "pk": row["id"],
        "updated_at": str(row["updated_at"]),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "data": payload,
    }


def write_lake_records(table, records):
    os.makedirs(LAKE_DIR, exist_ok=True)
    path = os.path.join(LAKE_DIR, f"{table}.jsonl")
    with open(path, "a") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return path


def run():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    total_changes = 0

    try:
        for table in TABLES:
            rows, new_cursor = extract_table(conn, table)

            if not rows:
                print(f"[{table}] no changes")
                continue

            records = [to_change_record(table, row) for row in rows]
            path = write_lake_records(table, records)

            # Only advance the checkpoint AFTER the lake write succeeds,
            # and commit both in the same transaction as the lake file
            # write already being on disk — this makes a crash here safe
            # to just re-run (lake write is append-only + de-duped by
            # downstream loaders on (table, pk, updated_at)).
            with conn.cursor() as cur:
                advance_checkpoint(cur, table, new_cursor[0], new_cursor[1])
            conn.commit()

            total_changes += len(records)
            print(f"[{table}] captured {len(records)} change(s) -> {path}")

        print(f"\nDone. {total_changes} total change record(s) captured this run.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
