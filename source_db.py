"""
Source system simulation.

Simulated CDC note (documented per assignment instructions):
------------------------------------------------------------
In production this would be PostgreSQL with logical replication (WAL) feeding
a Debezium-style connector. For local scope we simulate an equivalent
log-based CDC contract using SQLite: every DML call goes through this
wrapper's insert()/update()/delete() methods, which write the business row
AND an append-only change event into `cdc_log` in the SAME transaction. This
gives us the same guarantee a WAL gives us: no change can happen without a
corresponding durable, ordered, append-only log record.

cdc_log is the source's equivalent of a WAL/binlog: append-only, ordered by
log_id (monotonic), never updated or deleted.
"""
import sqlite3
import json
import datetime as dt
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "source.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    full_name       TEXT NOT NULL,
    country_code    TEXT NOT NULL,
    is_verified     INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallets (
    wallet_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    currency_code   TEXT NOT NULL,
    balance         REAL NOT NULL DEFAULT 0.0,
    status          TEXT NOT NULL CHECK (status IN ('ACTIVE','FROZEN','CLOSED')) DEFAULT 'ACTIVE',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_wallet_id    INTEGER REFERENCES wallets(wallet_id),
    destination_wallet_id INTEGER REFERENCES wallets(wallet_id),
    type                TEXT NOT NULL CHECK (type IN ('DEPOSIT','WITHDRAWAL','TRANSFER','REFUND')),
    status              TEXT NOT NULL CHECK (status IN ('PENDING','COMPLETED','FAILED','REVERSED')) DEFAULT 'PENDING',
    amount              REAL NOT NULL CHECK (amount > 0),
    currency_code       TEXT NOT NULL,
    initiated_at        TEXT NOT NULL,
    settled_at          TEXT,
    failure_reason       TEXT
);

CREATE TABLE IF NOT EXISTS transaction_line_items (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(transaction_id),
    line_number     INTEGER NOT NULL,
    description     TEXT NOT NULL,
    amount          REAL NOT NULL,
    fee_flag        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (transaction_id, line_number)
);

CREATE TABLE IF NOT EXISTS balance_history (
    wallet_id       INTEGER NOT NULL REFERENCES wallets(wallet_id),
    history_id      INTEGER NOT NULL,
    transaction_id  INTEGER REFERENCES transactions(transaction_id),
    delta_amount    REAL NOT NULL,
    balance_after   REAL NOT NULL CHECK (balance_after >= 0),
    recorded_at     TEXT NOT NULL,
    PRIMARY KEY (wallet_id, history_id)
);

-- Append-only change log == our simulated WAL / CDC source of truth
CREATE TABLE IF NOT EXISTS cdc_log (
    log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    op          TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
    pk_json     TEXT NOT NULL,
    row_json    TEXT NOT NULL,
    event_ts    TEXT NOT NULL
);
"""


def now() -> str:
    return dt.datetime.utcnow().isoformat()


class SourceDB:
    """Thin wrapper that guarantees every DML produces a cdc_log row."""

    def __init__(self, path: Path = DB_PATH):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def _log(self, table, op, pk: dict, row: dict):
        self.conn.execute(
            "INSERT INTO cdc_log (table_name, op, pk_json, row_json, event_ts) VALUES (?,?,?,?,?)",
            (table, op, json.dumps(pk, default=str), json.dumps(row, default=str), now()),
        )

    def insert(self, table: str, row: dict, pk_cols: list[str]):
        cols = list(row.keys())
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
        cur = self.conn.execute(sql, [row[c] for c in cols])
        full_row = dict(row)
        if "rowid" not in pk_cols and len(pk_cols) == 1 and pk_cols[0] not in row:
            full_row[pk_cols[0]] = cur.lastrowid
        pk = {c: full_row.get(c, cur.lastrowid) for c in pk_cols}
        self._log(table, "INSERT", pk, full_row)
        self.conn.commit()
        return cur.lastrowid

    def update(self, table: str, pk: dict, changes: dict):
        set_clause = ",".join(f"{c}=?" for c in changes)
        where_clause = " AND ".join(f"{c}=?" for c in pk)
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self.conn.execute(sql, list(changes.values()) + list(pk.values()))
        row = self.conn.execute(
            f"SELECT * FROM {table} WHERE {where_clause}", list(pk.values())
        ).fetchone()
        cols = [d[0] for d in self.conn.execute(f"SELECT * FROM {table} LIMIT 0").description]
        full_row = dict(zip(cols, row)) if row else changes
        self._log(table, "UPDATE", pk, full_row)
        self.conn.commit()

    def delete(self, table: str, pk: dict):
        where_clause = " AND ".join(f"{c}=?" for c in pk)
        self.conn.execute(f"DELETE FROM {table} WHERE {where_clause}", list(pk.values()))
        self._log(table, "DELETE", pk, pk)
        self.conn.commit()

    def close(self):
        self.conn.close()


def seed():
    db = SourceDB()
    ts = now()
    c1 = db.insert("customers", {
        "email": "asha@example.com", "full_name": "Asha Rao", "country_code": "IN",
        "is_verified": 1, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    c2 = db.insert("customers", {
        "email": "leo@example.com", "full_name": "Leo Fernandes", "country_code": "IN",
        "is_verified": 1, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])

    w1 = db.insert("wallets", {
        "customer_id": c1, "currency_code": "INR", "balance": 1000.0,
        "status": "ACTIVE", "created_at": ts, "updated_at": ts,
    }, ["wallet_id"])
    w2 = db.insert("wallets", {
        "customer_id": c2, "currency_code": "INR", "balance": 500.0,
        "status": "ACTIVE", "created_at": ts, "updated_at": ts,
    }, ["wallet_id"])

    t1 = db.insert("transactions", {
        "source_wallet_id": w1, "destination_wallet_id": w2, "type": "TRANSFER",
        "status": "COMPLETED", "amount": 250.0, "currency_code": "INR",
        "initiated_at": ts, "settled_at": ts, "failure_reason": None,
    }, ["transaction_id"])

    db.insert("transaction_line_items", {
        "transaction_id": t1, "line_number": 1, "description": "Transfer principal",
        "amount": 245.0, "fee_flag": 0,
    }, ["transaction_id", "line_number"])
    db.insert("transaction_line_items", {
        "transaction_id": t1, "line_number": 2, "description": "Platform fee",
        "amount": 5.0, "fee_flag": 1,
    }, ["transaction_id", "line_number"])

    db.insert("balance_history", {
        "wallet_id": w1, "history_id": 1, "transaction_id": t1,
        "delta_amount": -250.0, "balance_after": 750.0, "recorded_at": ts,
    }, ["wallet_id", "history_id"])
    db.insert("balance_history", {
        "wallet_id": w2, "history_id": 1, "transaction_id": t1,
        "delta_amount": 250.0, "balance_after": 750.0, "recorded_at": ts,
    }, ["wallet_id", "history_id"])

    # apply the balance change to wallets (mimics an app-layer transaction)
    db.update("wallets", {"wallet_id": w1}, {"balance": 750.0, "updated_at": now()})
    db.update("wallets", {"wallet_id": w2}, {"balance": 750.0, "updated_at": now()})

    db.close()
    print("Seeded source.db with 2 customers, 2 wallets, 1 transaction (2 line items), balance history.")


if __name__ == "__main__":
    seed()
