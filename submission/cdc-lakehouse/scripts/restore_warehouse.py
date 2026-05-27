#!/usr/bin/env python3
"""Rebuild warehouse snapshot from lake Parquet events as-of a timestamp."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.config import LAKE_DIR, WAREHOUSE_PATH
from pipeline.lake_writer import read_all_lake_events
from pipeline.warehouse_apply import (
    apply_events,
    create_warehouse_tables,
    truncate_warehouse_tables,
)


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def restore(as_of: datetime, lake_dir=None) -> int:
    events = read_all_lake_events(lake_dir or LAKE_DIR)
    filtered = [
        e
        for e in events
        if (
            e.event_ts.replace(tzinfo=timezone.utc)
            if e.event_ts.tzinfo is None
            else e.event_ts.astimezone(timezone.utc)
        )
        <= as_of.astimezone(timezone.utc)
    ]

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(WAREHOUSE_PATH))
    create_warehouse_tables(conn)
    truncate_warehouse_tables(conn)
    apply_events(conn, filtered)
    conn.close()

    print(
        f"Restored warehouse from {len(filtered)} lake events as-of {as_of.isoformat()}."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Point-in-time warehouse restore from lake"
    )
    parser.add_argument("--as-of", required=True, help="ISO-8601 timestamp (UTC)")
    args = parser.parse_args()
    return restore(_parse_ts(args.as_of))


if __name__ == "__main__":
    sys.exit(main())
