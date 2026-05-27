#!/usr/bin/env python3
"""Run CDC ingestion: schema check → lake → warehouse → checkpoint."""

from __future__ import annotations

import sys

import duckdb

from pipeline.checkpoint import load_checkpoint, save_checkpoint
from pipeline.config import WAREHOUSE_PATH
from pipeline.lake_writer import write_events_to_lake
from pipeline.mysql_client import (
    connect_mysql,
    fetch_cdc_events_since,
    fetch_information_schema,
)
from pipeline.schema_check import (
    export_contract_file,
    validate_schema,
    write_failure_artifact,
)
from pipeline.warehouse_apply import apply_events, create_warehouse_tables


def run_ingestion() -> int:
    export_contract_file()

    try:
        mysql = connect_mysql()
    except Exception as exc:
        print(f"Cannot connect to MySQL: {exc}", file=sys.stderr)
        return 1

    info_cols = fetch_information_schema(mysql)

    # Always validate live source against source-team contract before any ingest.
    violations = validate_schema(info_schema_columns=info_cols)
    if violations:
        write_failure_artifact(violations)
        print(
            "Schema validation failed — ingestion stopped (live source ≠ source contract).",
            file=sys.stderr,
        )
        for v in violations:
            print(f"  ✗ {v}", file=sys.stderr)
        return 1

    print("Schema validation passed (live source matches source contract).")

    last_event_id = load_checkpoint()
    events = fetch_cdc_events_since(mysql, last_event_id)
    if not events:
        print(f"No new CDC events after event_id={last_event_id}.")
        return 0

    write_events_to_lake(events)

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    wh_conn = duckdb.connect(str(WAREHOUSE_PATH))
    create_warehouse_tables(wh_conn)
    apply_events(wh_conn, events)
    wh_conn.close()

    new_checkpoint = max(e.event_id for e in events)
    save_checkpoint(new_checkpoint)
    print(f"Ingested {len(events)} events; checkpoint={new_checkpoint}.")
    return 0


def main() -> int:
    return run_ingestion()


if __name__ == "__main__":
    sys.exit(main())
