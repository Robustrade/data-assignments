"""
Warehouse Layer (DuckDB).

For every source table we maintain two warehouse structures:
  1. <table>_current   -- latest-state snapshot, one row per business key,
                           near-real-time, used by downstream analytics.
  2. <table>_history    -- SCD2 append-only versions: valid_from, valid_to,
                           is_current, is_deleted. This is what enables
                           "move backward in time" / point-in-time restore.

Load logic per event (idempotent, dedup by log_id):
  - Skip if log_id already applied (tracked in _applied_log_ids).
  - INSERT/UPDATE: close the previous open history version (set valid_to =
    event_ts, is_current = FALSE) if one exists, insert a new open version,
    and upsert the *_current row.
  - DELETE: close the open history version with is_deleted = TRUE and
    remove (or flag) the row in *_current.

Time travel: `SELECT * FROM <table>_history WHERE valid_from <= :ts AND
(valid_to IS NULL OR valid_to > :ts) AND wallet_id = :id` reconstructs the
row as of any past timestamp.
"""
import json
import duckdb
import datetime as dt
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "lake"
WAREHOUSE_PATH = BASE / "data" / "warehouse.duckdb"
WH_CHECKPOINT_PATH = BASE / "checkpoints" / "warehouse_checkpoint.json"

TABLE_PKS = {
    "customers": ["customer_id"],
    "wallets": ["wallet_id"],
    "transactions": ["transaction_id"],
    "transaction_line_items": ["transaction_id", "line_number"],
    "balance_history": ["wallet_id", "history_id"],
}


def load_wh_checkpoint() -> int:
    if WH_CHECKPOINT_PATH.exists():
        return json.loads(WH_CHECKPOINT_PATH.read_text())["last_applied_log_id"]
    return 0


def save_wh_checkpoint(log_id: int):
    WH_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    WH_CHECKPOINT_PATH.write_text(json.dumps({"last_applied_log_id": log_id}))


def _pk_key(pk: dict, pk_cols: list[str]) -> str:
    return "|".join(str(pk[c]) for c in pk_cols)


def _ensure_tables(con: duckdb.DuckDBPyConnection, table: str):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table}_current (
            pk_key VARCHAR PRIMARY KEY,
            row_data JSON,
            updated_at TIMESTAMP
        );
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table}_history (
            version_id BIGINT,
            pk_key VARCHAR,
            row_data JSON,
            op VARCHAR,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN,
            is_deleted BOOLEAN
        );
    """)
    con.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS seq_{table}_version START 1;
    """)


def _read_all_lake_events() -> list[dict]:
    events = []
    if not LAKE_DIR.exists():
        return events
    for jsonl_file in LAKE_DIR.glob("*/dt=*/changes.jsonl"):
        with open(jsonl_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    events.sort(key=lambda e: e["log_id"])
    return events


def apply_events() -> dict:
    con = duckdb.connect(str(WAREHOUSE_PATH))
    con.execute("CREATE TABLE IF NOT EXISTS _applied_log_ids (log_id BIGINT PRIMARY KEY);")

    applied_ids = {r[0] for r in con.execute("SELECT log_id FROM _applied_log_ids").fetchall()}

    events = _read_all_lake_events()
    last_checkpoint = load_wh_checkpoint()
    applied_count = 0
    max_log_id = last_checkpoint

    for tbl in TABLE_PKS:
        _ensure_tables(con, tbl)

    for ev in events:
        log_id = ev["log_id"]
        if log_id in applied_ids:
            continue  # dedup: already-applied event, safe to skip (replay safety)

        table = ev["table_name"]
        op = ev["op"]
        pk_cols = TABLE_PKS[table]
        pk_key = _pk_key(ev["pk"], pk_cols)
        row_json = json.dumps(ev["row"], default=str)
        event_ts = ev["event_ts"]

        # close currently-open history version, if any
        con.execute(
            f"UPDATE {table}_history SET valid_to = ?, is_current = FALSE "
            f"WHERE pk_key = ? AND is_current = TRUE",
            [event_ts, pk_key],
        )

        version_id = con.execute(f"SELECT nextval('seq_{table}_version')").fetchone()[0]

        if op in ("INSERT", "UPDATE"):
            con.execute(
                f"INSERT INTO {table}_history VALUES (?,?,?,?,?,?,?,?)",
                [version_id, pk_key, row_json, op, event_ts, None, True, False],
            )
            con.execute(f"DELETE FROM {table}_current WHERE pk_key = ?", [pk_key])
            con.execute(
                f"INSERT INTO {table}_current VALUES (?,?,?)",
                [pk_key, row_json, event_ts],
            )
        elif op == "DELETE":
            con.execute(
                f"INSERT INTO {table}_history VALUES (?,?,?,?,?,?,?,?)",
                [version_id, pk_key, row_json, op, event_ts, None, False, True],
            )
            con.execute(f"DELETE FROM {table}_current WHERE pk_key = ?", [pk_key])

        con.execute("INSERT INTO _applied_log_ids VALUES (?)", [log_id])
        applied_ids.add(log_id)
        applied_count += 1
        max_log_id = max(max_log_id, log_id)

    if applied_count > 0:
        save_wh_checkpoint(max_log_id)

    con.close()
    return {"events_applied": applied_count, "checkpoint_after": max_log_id}


def reconstruct_at(table: str, pk_key: str, as_of_ts: str, db_path=WAREHOUSE_PATH) -> dict | None:
    """Point-in-time restore: reconstruct a row's state as of as_of_ts."""
    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute(
        f"""
        SELECT row_data, op, is_deleted FROM {table}_history
        WHERE pk_key = ? AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to > ?)
        ORDER BY version_id DESC LIMIT 1
        """,
        [pk_key, as_of_ts, as_of_ts],
    ).fetchone()
    con.close()
    if row is None or row[2]:  # not found or was deleted at that point
        return None
    return json.loads(row[0])


if __name__ == "__main__":
    summary = apply_events()
    print(f"Warehouse load OK: {summary}")
