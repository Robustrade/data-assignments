import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import TABLES

def now():
    return datetime.now(timezone.utc).isoformat()

def connect(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con

def init_source(db_path: Path, schema_sql: Path):
    con = connect(db_path)
    con.executescript(schema_sql.read_text())
    con.commit()
    con.close()

def insert_seed_data(db_path: Path):
    con = connect(db_path)
    ts = now()

    # Customers
    con.executemany(
        "INSERT OR IGNORE INTO customers VALUES (?,?,?,?,?,?)",
        [
            (1, "a@example.com", "Alice", "ACTIVE", ts, ts),
            (2, "b@example.com", "Bob", "ACTIVE", ts, ts),
        ],
    )

    # Products
    con.executemany(
        "INSERT OR IGNORE INTO products VALUES (?,?,?,?,?,?,?,?)",
        [
            (101, "SKU-101", "Keyboard", 50.00, "USD", "ACTIVE", ts, ts),
            (102, "SKU-102", "Mouse", 25.00, "USD", "ACTIVE", ts, ts),
        ],
    )

    # Orders
    con.execute(
        "INSERT OR IGNORE INTO orders VALUES (?,?,?,?,?,?,?)",
        (1001, 1, "PENDING", 75.00, "USD", ts, ts),
    )

    # Order items
    con.executemany(
        "INSERT OR IGNORE INTO order_items VALUES (?,?,?,?,?)",
        [
            (1001, 1, 101, 1, 50.00),
            (1001, 2, 102, 1, 25.00),
        ],
    )

    # Payment
    con.execute(
        "INSERT OR IGNORE INTO payments VALUES (?,?,?,?,?,?)",
        (5001, 1001, 75.00, "AUTHORIZED", None, ts),
    )

    con.commit()
    con.close()
def schema_snapshot(db_path: Path):
    con = connect(db_path)
    result = {}
    for table in TABLES:
        cols = con.execute(f"PRAGMA table_info({table})").fetchall()
        result[table] = [
            {"name": c["name"], "type": c["type"], "notnull": bool(c["notnull"]),
             "pk": int(c["pk"])}
            for c in cols
        ]
    con.close()
    return result
