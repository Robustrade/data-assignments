"""
Schema Change Detection and Safe Stop.

Strategy:
- Fingerprint the source schema (table -> [(col, type, notnull, pk)]) using
  PRAGMA table_info, which is SQLite's catalog equivalent of
  information_schema.columns.
- Persist the last-known-good fingerprint to checkpoints/schema_fingerprint.json.
- On every pipeline run, BEFORE any CDC extraction happens, compare current
  fingerprint against last-known-good.
- Any of the following is classified INCOMPATIBLE and halts ingestion:
    * a previously-existing column is missing (dropped OR renamed)
    * an existing column's declared type changed
    * an existing column's NOT NULL-ness changed
    * an existing table disappeared
- New tables or new nullable columns are treated as backward-compatible and
  do NOT halt ingestion (documented assumption / simplification).
- On incompatible change: raise SchemaDriftError, write a structured alert
  to checkpoints/schema_alerts.log, and the caller (cdc_extractor) must not
  proceed to write to the lake or warehouse.
"""
import json
import sqlite3
import datetime as dt
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
FINGERPRINT_PATH = BASE / "checkpoints" / "schema_fingerprint.json"
ALERT_LOG_PATH = BASE / "checkpoints" / "schema_alerts.log"

TRACKED_TABLES = [
    "customers", "wallets", "transactions",
    "transaction_line_items", "balance_history",
]


class SchemaDriftError(Exception):
    pass


def fingerprint(conn: sqlite3.Connection) -> dict:
    fp = {}
    for table in TRACKED_TABLES:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        # cid, name, type, notnull, dflt_value, pk
        fp[table] = sorted(
            [{"name": c[1], "type": c[2], "notnull": c[3], "pk": c[5]} for c in cols],
            key=lambda x: x["name"],
        )
    return fp


def load_known_good() -> dict | None:
    if FINGERPRINT_PATH.exists():
        return json.loads(FINGERPRINT_PATH.read_text())
    return None


def save_known_good(fp: dict):
    FINGERPRINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    FINGERPRINT_PATH.write_text(json.dumps(fp, indent=2))


def _alert(message: str):
    ALERT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERT_LOG_PATH, "a") as f:
        f.write(f"[{dt.datetime.utcnow().isoformat()}] SCHEMA_DRIFT_HALT: {message}\n")


def check(conn: sqlite3.Connection) -> dict:
    """
    Returns the current fingerprint if compatible (or first run).
    Raises SchemaDriftError and writes an alert if an incompatible change
    is detected. Caller must NOT proceed with ingestion if this raises.
    """
    current = fingerprint(conn)
    known_good = load_known_good()

    if known_good is None:
        save_known_good(current)
        return current

    problems = []
    for table, old_cols in known_good.items():
        new_cols = {c["name"]: c for c in current.get(table, [])}
        old_by_name = {c["name"]: c for c in old_cols}

        if table not in current:
            problems.append(f"table '{table}' no longer exists in source")
            continue

        for name, old_col in old_by_name.items():
            if name not in new_cols:
                problems.append(f"{table}.{name}: column dropped or renamed")
                continue
            new_col = new_cols[name]
            if old_col["type"] != new_col["type"]:
                problems.append(
                    f"{table}.{name}: type changed {old_col['type']} -> {new_col['type']}"
                )
            if old_col["notnull"] != new_col["notnull"]:
                problems.append(
                    f"{table}.{name}: NOT NULL changed {old_col['notnull']} -> {new_col['notnull']}"
                )
            if old_col["pk"] != new_col["pk"]:
                problems.append(
                    f"{table}.{name}: primary-key role changed {old_col['pk']} -> {new_col['pk']}"
                )

    if problems:
        msg = "; ".join(problems)
        _alert(msg)
        raise SchemaDriftError(
            f"Incompatible source schema change detected. Ingestion HALTED. Details: {msg}"
        )

    # Compatible (possibly with additive, non-breaking changes) -> advance baseline
    save_known_good(current)
    return current
