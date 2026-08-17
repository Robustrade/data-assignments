"""
CDC / Ingestion Layer.

- Runs schema_guard.check() FIRST. If it raises, extraction stops immediately
  and nothing is written to the lake (stop-the-line behavior).
- Reads cdc_log rows with log_id > last checkpoint (checkpoints/cdc_checkpoint.json).
- Writes each event as a line in an append-only, date-partitioned JSONL file
  under lake/<table_name>/dt=YYYY-MM-DD/changes.jsonl.
- Advances the checkpoint only after a successful, flushed write (at-least-once
  delivery to the lake; downstream warehouse load step dedupes on log_id, so
  the overall pipeline is effectively exactly-once for the warehouse state).
- Replay-safe: re-running with an unchanged or rolled-back checkpoint will
  re-emit already-seen log_ids into the lake (harmless, since the lake is an
  append log and dedup happens at load time) -- this satisfies "duplicate
  CDC events" and "retries after partial failure" requirements.
"""
import json
import sqlite3
import datetime as dt
from pathlib import Path

from schema_guard import check as schema_check, SchemaDriftError

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "source.db"
LAKE_DIR = BASE / "lake"
CHECKPOINT_PATH = BASE / "checkpoints" / "cdc_checkpoint.json"


def load_checkpoint() -> int:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_text())["last_log_id"]
    return 0


def save_checkpoint(log_id: int):
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(json.dumps({"last_log_id": log_id}))


def extract(reset_checkpoint_to: int | None = None) -> dict:
    """
    Runs one extraction cycle. Returns a summary dict.
    Raises SchemaDriftError if the source schema has drifted incompatibly --
    caller is responsible for treating that as a hard stop.
    """
    conn = sqlite3.connect(DB_PATH)

    # 1) Safety gate: must pass before we touch cdc_log or the lake at all.
    schema_check(conn)  # raises SchemaDriftError on incompatible drift

    # 2) Determine starting offset
    last_log_id = reset_checkpoint_to if reset_checkpoint_to is not None else load_checkpoint()

    rows = conn.execute(
        "SELECT log_id, table_name, op, pk_json, row_json, event_ts "
        "FROM cdc_log WHERE log_id > ? ORDER BY log_id ASC",
        (last_log_id,),
    ).fetchall()

    written = 0
    max_log_id = last_log_id
    per_table_dates = {}

    for log_id, table_name, op, pk_json, row_json, event_ts in rows:
        event_date = event_ts[:10]
        out_dir = LAKE_DIR / table_name / f"dt={event_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "changes.jsonl"

        record = {
            "log_id": log_id,
            "table_name": table_name,
            "op": op,
            "pk": json.loads(pk_json),
            "row": json.loads(row_json),
            "event_ts": event_ts,
            "extracted_at": dt.datetime.utcnow().isoformat(),
        }
        with open(out_file, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")

        written += 1
        max_log_id = max(max_log_id, log_id)
        per_table_dates.setdefault(table_name, set()).add(event_date)

    if written > 0:
        save_checkpoint(max_log_id)

    conn.close()
    return {
        "events_written": written,
        "checkpoint_before": last_log_id,
        "checkpoint_after": max_log_id,
    }


if __name__ == "__main__":
    try:
        summary = extract()
        print(f"CDC extraction OK: {summary}")
    except SchemaDriftError as e:
        print(f"CDC extraction HALTED: {e}")
        raise SystemExit(1)
