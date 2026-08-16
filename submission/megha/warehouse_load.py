#!/usr/bin/env python3
"""
warehouse_load.py — loads lake change records into the warehouse.

Reads each lake/{table}.jsonl file, and for every record not already in
warehouse.load_log (keyed by table+pk+updated_at, so replays are a no-op):
  1. Upserts the current-state snapshot table (warehouse.{table}).
  2. For wallet/transfer: closes out the previous SCD2 row (sets valid_to,
     is_current=false) and inserts a new current row.
  3. Records the (table, pk, updated_at) in load_log.

All three steps for a given record happen in one transaction, so a crash
mid-load never leaves the snapshot and history tables inconsistent.

Usage:
    python3 warehouse_load.py
"""

import json
import os

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

SNAPSHOT_COLUMNS = {
    "customer": ["id", "name", "email", "created_at", "updated_at", "is_deleted"],
    "wallet": ["id", "customer_id", "currency", "status", "created_at", "updated_at", "is_deleted"],
    "transfer": ["id", "source_wallet_id", "dest_wallet_id", "amount", "status",
                 "created_at", "settled_at", "updated_at", "is_deleted"],
    "wallet_balance_history": ["id", "wallet_id", "balance_after", "recorded_at",
                                "updated_at", "is_deleted"],
    "payment_attempt": ["id", "transfer_id", "attempt_no", "status", "failure_reason",
                         "created_at", "updated_at", "is_deleted"],
}

SCD2_TABLES = {"wallet", "transfer"}

SCD2_COLUMNS = {
    "wallet": ["id", "customer_id", "currency", "status", "is_deleted"],
    "transfer": ["id", "source_wallet_id", "dest_wallet_id", "amount", "status",
                 "settled_at", "is_deleted"],
}


def already_loaded(cur, table, pk, updated_at):
    cur.execute(
        """
        SELECT 1 FROM warehouse.load_log
        WHERE table_name = %s AND pk = %s AND updated_at = %s
        """,
        (table, pk, updated_at),
    )
    return cur.fetchone() is not None


def upsert_snapshot(cur, table, data):
    cols = SNAPSHOT_COLUMNS[table]
    values = [data.get(c) for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "id")
    cur.execute(
        f"""
        INSERT INTO warehouse.{table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
        """,
        values,
    )


def upsert_scd2(cur, table, data):
    history_table = f"warehouse.{table}_history"
    cols = SCD2_COLUMNS[table]
    entity_id = data["id"]
    valid_from = data["updated_at"]

    # Close out the current row, if one exists and actually differs.
    cur.execute(
        f"SELECT {', '.join(cols)} FROM {history_table} WHERE id = %s AND is_current = true",
        (entity_id,),
    )
    current = cur.fetchone()

    new_values = tuple(str(data.get(c)) for c in cols)
    if current is not None and tuple(str(v) for v in current) == new_values:
        # No actual change in tracked columns (e.g. metadata-only touch) — skip.
        return

    if current is not None:
        cur.execute(
            f"""
            UPDATE {history_table}
            SET valid_to = %s, is_current = false
            WHERE id = %s AND is_current = true
            """,
            (valid_from, entity_id),
        )

    values = [data.get(c) for c in cols] + [valid_from]
    placeholders = ", ".join(["%s"] * len(cols))
    cur.execute(
        f"""
        INSERT INTO {history_table} ({", ".join(cols)}, valid_from, is_current)
        VALUES ({placeholders}, %s, true)
        """,
        values,
    )


def load_table(conn, table):
    path = os.path.join(LAKE_DIR, f"{table}.jsonl")
    if not os.path.exists(path):
        print(f"[{table}] no lake file, skipping")
        return 0

    loaded = 0
    with open(path) as f:
        lines = f.readlines()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for line in lines:
            record = json.loads(line)
            data = record["data"]
            pk = record["pk"]
            updated_at = record["updated_at"]

            if already_loaded(cur, table, pk, updated_at):
                continue

            upsert_snapshot(cur, table, data)

            if table in SCD2_TABLES:
                upsert_scd2(cur, table, data)

            cur.execute(
                """
                INSERT INTO warehouse.load_log (table_name, pk, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (table, pk, updated_at),
            )
            loaded += 1

    conn.commit()
    return loaded


def run():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    total = 0

    try:
        for table in SNAPSHOT_COLUMNS:
            n = load_table(conn, table)
            print(f"[{table}] loaded {n} new record(s)")
            total += n
        print(f"\nDone. {total} total record(s) loaded into warehouse.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
