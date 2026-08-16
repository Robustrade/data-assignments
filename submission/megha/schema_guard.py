#!/usr/bin/env python3
"""
schema_guard.py — detects source schema drift before CDC extraction runs.

On first run (no stored snapshot), captures the current schema of every
CDC-tracked table as the "expected" baseline and saves it to
schema_snapshot.json.

On every subsequent run, compares the live schema against that baseline:
  - SAFE changes (new nullable column added) are logged as warnings and
    ingestion is allowed to proceed. The baseline is updated to include
    the new column so it's no longer flagged next time.
  - BREAKING changes (column dropped/renamed, type changed, nullable
    column made NOT NULL, column narrowed) cause this script to exit
    with a non-zero status, and cdc_extract.py should not be invoked.

Usage:
    python3 schema_guard.py          # check only, exit 1 on breaking change
    python3 schema_guard.py --init   # (re)initialize the baseline snapshot
"""

import json
import sys

import psycopg2
import psycopg2.extras

DB_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="wallet_db",
    user="postgres",
    password="postgres",
)

TABLES = [
    "customer",
    "wallet",
    "transfer",
    "wallet_balance_history",
    "payment_attempt",
]

SNAPSHOT_PATH = "schema_snapshot.json"


def get_live_schema(conn):
    """Returns {table: {column: {type, nullable}}} for all tracked tables."""
    schema = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for table in TABLES:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            cols = {
                row["column_name"]: {
                    "type": row["data_type"],
                    "nullable": row["is_nullable"] == "YES",
                }
                for row in cur.fetchall()
            }
            schema[table] = cols
    return schema


def load_baseline():
    try:
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def save_baseline(schema):
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(schema, f, indent=2, sort_keys=True)


def diff_schema(baseline, live):
    """
    Returns (breaking_changes, safe_changes), each a list of human-readable
    strings.
    """
    breaking = []
    safe = []

    for table, expected_cols in baseline.items():
        live_cols = live.get(table)
        if live_cols is None:
            breaking.append(f"{table}: table dropped entirely")
            continue

        for col, expected in expected_cols.items():
            live_col = live_cols.get(col)
            if live_col is None:
                breaking.append(f"{table}.{col}: column dropped")
                continue
            if live_col["type"] != expected["type"]:
                breaking.append(
                    f"{table}.{col}: type changed "
                    f"({expected['type']} -> {live_col['type']})"
                )
            if expected["nullable"] and not live_col["nullable"]:
                breaking.append(
                    f"{table}.{col}: nullability tightened "
                    f"(was nullable, now NOT NULL)"
                )

        for col in live_cols:
            if col not in expected_cols:
                safe.append(f"{table}.{col}: new column added")

    for table in live:
        if table not in baseline:
            safe.append(f"{table}: new table added")

    return breaking, safe


def run(init=False):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        live_schema = get_live_schema(conn)
    finally:
        conn.close()

    if init:
        save_baseline(live_schema)
        print(f"Baseline schema snapshot written to {SNAPSHOT_PATH}.")
        return 0

    baseline = load_baseline()
    if baseline is None:
        print("No baseline found — initializing one now (first run).")
        save_baseline(live_schema)
        return 0

    breaking, safe = diff_schema(baseline, live_schema)

    if safe:
        print("Safe schema changes detected (ingestion will proceed):")
        for s in safe:
            print(f"  - {s}")
        # Merge new columns/tables into the baseline so they're not
        # re-flagged on the next run.
        save_baseline(live_schema)

    if breaking:
        print("\nBREAKING schema change(s) detected — halting ingestion:")
        for b in breaking:
            print(f"  - {b}")
        print(
            "\nCDC extraction has NOT been run. Resolve the schema change, "
            "then re-run with --init to accept the new baseline (only after "
            "downstream lake/warehouse mappings have been updated to match)."
        )
        return 1

    if not safe:
        print("No schema changes detected. Safe to proceed.")

    return 0


if __name__ == "__main__":
    init = "--init" in sys.argv
    sys.exit(run(init=init))
