import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS applied_events (
    event_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_lsn INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY, email TEXT NOT NULL, full_name TEXT NOT NULL,
    status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY, sku TEXT NOT NULL, name TEXT NOT NULL,
    unit_price REAL NOT NULL, currency TEXT NOT NULL, status TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, status TEXT NOT NULL,
    order_total REAL NOT NULL, currency TEXT NOT NULL, ordered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_items (
    order_id INTEGER NOT NULL, line_number INTEGER NOT NULL, product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL, unit_price REAL NOT NULL,
    PRIMARY KEY(order_id, line_number)
);
CREATE TABLE IF NOT EXISTS payments (
    payment_id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, amount REAL NOT NULL,
    status TEXT NOT NULL, paid_at TEXT, created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS row_history (
    table_name TEXT NOT NULL,
    primary_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    row_json TEXT,
    source_lsn INTEGER NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    is_current INTEGER NOT NULL,
    PRIMARY KEY(table_name, primary_key, source_lsn)
);
"""

TABLE_COLUMNS = {
    "customers": ["customer_id","email","full_name","status","created_at","updated_at"],
    "products": ["product_id","sku","name","unit_price","currency","status","created_at","updated_at"],
    "orders": ["order_id","customer_id","status","order_total","currency","ordered_at","updated_at"],
    "order_items": ["order_id","line_number","product_id","quantity","unit_price"],
    "payments": ["payment_id","order_id","amount","status","paid_at","created_at"],
}
PK = {
    "customers": ["customer_id"], "products": ["product_id"], "orders": ["order_id"],
    "order_items": ["order_id","line_number"], "payments": ["payment_id"]
}

class Warehouse:
    def __init__(self, path):
        self.con = sqlite3.connect(path)
        self.con.row_factory = sqlite3.Row
        self.con.executescript(DDL)

    def _pk(self, event):
        return "|".join(str(event["primary_key"][x]) for x in PK[event["table_name"]])

    def apply(self, event):
        eid = event["event_id"]
        if self.con.execute("SELECT 1 FROM applied_events WHERE event_id=?", (eid,)).fetchone():
            return "duplicate"

        table = event["table_name"]
        pk = self._pk(event)
        latest = self.con.execute(
            "SELECT source_lsn FROM row_history WHERE table_name=? AND primary_key=? "
            "AND is_current=1", (table, pk)
        ).fetchone()
        if latest and event["source_lsn"] < latest["source_lsn"]:
            return "stale"

        if latest:
            self.con.execute(
                "UPDATE row_history SET valid_to=?, is_current=0 "
                "WHERE table_name=? AND primary_key=? AND is_current=1",
                (event["changed_at"], table, pk)
            )

        if event["operation"] == "DELETE":
            self.con.execute(
                f"DELETE FROM {table} WHERE " +
                " AND ".join(f"{c}=?" for c in PK[table]),
                tuple(event["primary_key"][c] for c in PK[table])
            )
        else:
            row = event["row_data"]
            cols = TABLE_COLUMNS[table]
            placeholders = ",".join("?" for _ in cols)
            updates = ",".join(f"{c}=excluded.{c}" for c in cols if c not in PK[table])
            sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) " \
                  f"ON CONFLICT({','.join(PK[table])}) DO UPDATE SET {updates}"
            self.con.execute(sql, tuple(row[c] for c in cols))

        import json
        self.con.execute(
            "INSERT INTO row_history VALUES (?,?,?,?,?,?,?,?)",
            (table, pk, event["operation"],
             json.dumps(event["row_data"], sort_keys=True) if event["row_data"] else None,
             event["source_lsn"], event["changed_at"], None, 1)
        )
        self.con.execute(
            "INSERT INTO applied_events VALUES (?,?,?)",
            (eid, table, event["source_lsn"])
        )
        self.con.commit()
        return "applied"

    def state_as_of(self, table, timestamp):
        import json
        rows = self.con.execute(
            "SELECT * FROM row_history WHERE table_name=? AND valid_from <= ? "
            "AND (valid_to IS NULL OR valid_to > ?) ORDER BY primary_key",
            (table, timestamp, timestamp)
        ).fetchall()
        return [json.loads(r["row_json"]) for r in rows if r["operation"] != "DELETE"]

    def close(self):
        self.con.close()
