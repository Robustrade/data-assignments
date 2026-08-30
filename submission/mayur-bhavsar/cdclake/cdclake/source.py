"""Source layer — the transactional system of record.

Owns the SQLite database, its DDL, deterministic seed data, a scripted change
workload, and a library of schema mutations used to exercise the schema gate.

The source is deliberately independent of ``contracts/``: nothing here reads a
contract file. That independence is what makes the schema gate meaningful.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from cdclake.config import SQL_DIR, TRACKED_TABLES

DDL_FILES = ("001_source_schema.sql", "002_cdc_triggers.sql")

#: Fixed base instant so seeded business timestamps are reproducible across
#: runs. CDC ``committed_at`` values still come from the database clock.
BASE_DAY = "2026-08-01"


def _ts(day_offset: int, hour: int = 9, minute: int = 0) -> str:
    day = 1 + day_offset
    return f"2026-08-{day:02d}T{hour:02d}:{minute:02d}:00.000Z"


# ── connection management ───────────────────────────────────────────────────
def connect(path: Path | str) -> sqlite3.Connection:
    """Open the source database with the pragmas this design depends on.

    ``foreign_keys``     — referential integrity is enforced, not assumed.
    ``recursive_triggers`` — without it SQLite does NOT fire delete triggers
                             for rows removed by ON DELETE CASCADE, so cascaded
                             child deletes would silently never reach the lake.
                             This is a real CDC correctness trap; see docs/02.
    """
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA recursive_triggers = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def apply_ddl(conn: sqlite3.Connection) -> None:
    for name in DDL_FILES:
        conn.executescript((SQL_DIR / name).read_text(encoding="utf-8"))


def create_source(path: Path | str) -> sqlite3.Connection:
    conn = connect(path)
    apply_ddl(conn)
    return conn


# ── small write helpers ─────────────────────────────────────────────────────
def insert(conn: sqlite3.Connection, table: str, **values: Any) -> None:
    cols = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    conn.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({marks})",  # nosec B608
        tuple(values.values()),
    )


def update(
    conn: sqlite3.Connection, table: str, where: dict[str, Any], **values: Any
) -> None:
    sets = ", ".join(f"{c} = ?" for c in values)
    cond = " AND ".join(f"{c} = ?" for c in where)
    conn.execute(
        f"UPDATE {table} SET {sets} WHERE {cond}",  # nosec B608
        tuple(values.values()) + tuple(where.values()),
    )


def delete(conn: sqlite3.Connection, table: str, **where: Any) -> None:
    cond = " AND ".join(f"{c} = ?" for c in where)
    sql = f"DELETE FROM {table} WHERE {cond}"  # nosec B608
    conn.execute(sql, tuple(where.values()))


def snapshot(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    """Current contents of a source table, for reconciliation against the
    warehouse."""
    cur = conn.execute(f"SELECT * FROM {table}")  # nosec B608
    return [dict(row) for row in cur.fetchall()]


def row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]  # nosec B608
        for t in TRACKED_TABLES
    }


# ── seed data ───────────────────────────────────────────────────────────────
def seed(conn: sqlite3.Connection) -> None:
    """Deterministic starting state: 4 customers, 5 products, 4 orders."""
    t0 = _ts(0)

    customers = [
        (1, "ada@example.com", "Ada Lovelace", "ACTIVE", "+44-20-7946-0001"),
        (2, "grace@example.com", "Grace Hopper", "ACTIVE", None),
        (3, "alan@example.com", "Alan Turing", "SUSPENDED", "+44-20-7946-0003"),
        (4, "katherine@example.com", "Katherine Johnson", "ACTIVE", None),
    ]
    for cid, email, name, status, phone in customers:
        insert(
            conn,
            "customer",
            customer_id=cid,
            email=email,
            full_name=name,
            status=status,
            phone=phone,
            created_at=t0,
            updated_at=t0,
        )

    products = [
        (1, "SKU-KEYB-01", "Mechanical Keyboard", 129.00),
        (2, "SKU-MOUS-01", "Ergonomic Mouse", 59.50),
        (3, "SKU-MON-27", '27" Monitor', 349.99),
        (4, "SKU-DOCK-01", "USB-C Dock", 189.25),
        (5, "SKU-CBL-02", "Thunderbolt Cable", 24.00),
    ]
    for pid, sku, name, price in products:
        insert(
            conn,
            "product",
            product_id=pid,
            sku=sku,
            name=name,
            unit_price=price,
            currency="GBP",
            discontinued_at=None,
            created_at=t0,
            updated_at=t0,
        )

    # order_id -> (customer_id, [(line_no, product_id, qty, unit_price)])
    orders: dict[int, tuple[int, list[tuple[int, int, int, float]]]] = {
        1001: (1, [(1, 1, 1, 129.00), (2, 5, 2, 24.00)]),
        1002: (2, [(1, 3, 2, 349.99)]),
        1003: (1, [(1, 2, 1, 59.50), (2, 4, 1, 189.25)]),
        1004: (4, [(1, 5, 4, 24.00)]),
    }
    for order_id, (customer_id, lines) in orders.items():
        total = round(sum(qty * price for _, _, qty, price in lines), 2)
        placed = _ts(1, 10, order_id % 60)
        insert(
            conn,
            "customer_order",
            order_id=order_id,
            customer_id=customer_id,
            status="PENDING",
            currency="GBP",
            total_amount=total,
            placed_at=placed,
            cancelled_at=None,
            created_at=placed,
            updated_at=placed,
        )
        for line_no, product_id, qty, price in lines:
            insert(
                conn,
                "order_item",
                order_id=order_id,
                line_no=line_no,
                product_id=product_id,
                quantity=qty,
                unit_price=price,
                extended_amount=round(qty * price, 2),
                created_at=placed,
                updated_at=placed,
            )
        insert(
            conn,
            "payment",
            payment_id=order_id * 10,
            order_id=order_id,
            method="CARD",
            amount=total,
            currency="GBP",
            status="INITIATED",
            created_at=placed,
            settled_at=None,
            updated_at=placed,
        )


# ── scripted change workload ────────────────────────────────────────────────
def workload_settle_payments(conn: sqlite3.Connection) -> None:
    """Happy path: gateway attempt succeeds, payment settles, order becomes
    PAID."""
    for order_id in (1001, 1002):
        payment_id = order_id * 10
        settled = _ts(2, 11, 0)
        insert(
            conn,
            "payment_attempt",
            payment_id=payment_id,
            attempt_no=1,
            result="SUCCESS",
            failure_code=None,
            attempted_at=settled,
            created_at=settled,
            updated_at=settled,
        )
        update(
            conn,
            "payment",
            {"payment_id": payment_id},
            status="SETTLED",
            settled_at=settled,
            updated_at=settled,
        )
        update(
            conn,
            "customer_order",
            {"order_id": order_id},
            status="PAID",
            updated_at=settled,
        )


def workload_retry_storm(conn: sqlite3.Connection) -> None:
    """High-churn path on the weak entity: two failed attempts then a
    success."""
    payment_id = 1003 * 10
    for attempt_no, (result, code) in enumerate(
        [("DECLINED", "51"), ("TIMEOUT", "GW_TIMEOUT"), ("SUCCESS", None)], start=1
    ):
        at = _ts(2, 12, attempt_no)
        insert(
            conn,
            "payment_attempt",
            payment_id=payment_id,
            attempt_no=attempt_no,
            result=result,
            failure_code=code,
            attempted_at=at,
            created_at=at,
            updated_at=at,
        )
    settled = _ts(2, 12, 30)
    update(
        conn,
        "payment",
        {"payment_id": payment_id},
        status="SETTLED",
        settled_at=settled,
        updated_at=settled,
    )
    update(
        conn, "customer_order", {"order_id": 1003}, status="PAID", updated_at=settled
    )


def workload_price_change(conn: sqlite3.Connection) -> None:
    """A product price edit must not retroactively change historical lines."""
    at = _ts(3, 9, 0)
    update(conn, "product", {"product_id": 1}, unit_price=139.00, updated_at=at)


def workload_ship_and_deliver(conn: sqlite3.Connection) -> None:
    for order_id, status, hour in ((1001, "SHIPPED", 14), (1002, "SHIPPED", 15)):
        update(
            conn,
            "customer_order",
            {"order_id": order_id},
            status=status,
            updated_at=_ts(3, hour, 0),
        )
    update(
        conn,
        "customer_order",
        {"order_id": 1001},
        status="DELIVERED",
        updated_at=_ts(4, 9, 0),
    )


def workload_cancellation(conn: sqlite3.Connection) -> None:
    """Cancel an unpaid order; the CHECK constraints force a consistent
    shape."""
    at = _ts(4, 10, 0)
    update(
        conn,
        "customer_order",
        {"order_id": 1004},
        status="CANCELLED",
        cancelled_at=at,
        updated_at=at,
    )
    update(
        conn,
        "payment",
        {"payment_id": 10040},
        status="FAILED",
        updated_at=at,
    )


def workload_deletes(conn: sqlite3.Connection) -> None:
    """Hard deletes, including a cascade that must reach the lake as child
    delete events.

    Note the ordering: payment does NOT cascade from customer_order, because
    deleting an order should never silently destroy financial records. The
    erasure therefore has to walk the graph explicitly, and the foreign key
    enforces that. order_item, being a weak entity, does cascade.
    """
    delete(conn, "payment_attempt", payment_id=10030, attempt_no=1)
    delete(conn, "payment", payment_id=10040)
    delete(conn, "customer_order", order_id=1004)  # cascades to order_item


def workload_soft_churn(conn: sqlite3.Connection) -> None:
    """A customer edit plus a same-key rapid update, to exercise version
    ordering."""
    at = _ts(5, 9, 0)
    update(conn, "customer", {"customer_id": 3}, status="ACTIVE", updated_at=at)
    update(
        conn,
        "customer",
        {"customer_id": 3},
        phone="+44-20-7946-9999",
        updated_at=_ts(5, 9, 1),
    )


#: Ordered workload used by the demo and the end-to-end scripts.
WORKLOAD_PHASES: tuple[tuple[str, Any], ...] = (
    ("settle_payments", workload_settle_payments),
    ("retry_storm", workload_retry_storm),
    ("price_change", workload_price_change),
    ("ship_and_deliver", workload_ship_and_deliver),
    ("cancellation", workload_cancellation),
    ("soft_churn", workload_soft_churn),
    ("deletes", workload_deletes),
)


def run_workload(conn: sqlite3.Connection, phases: Iterable[str] | None = None) -> None:
    wanted = set(phases) if phases is not None else None
    for name, fn in WORKLOAD_PHASES:
        if wanted is None or name in wanted:
            fn(conn)


# ── schema mutations (used to exercise the gate) ────────────────────────────
def _table_ddl(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    if row is None:
        raise ValueError(f"unknown table {table!r}")
    return str(row["sql"])


def _objects_for(conn: sqlite3.Connection, table: str, kind: str) -> list[str]:
    rows = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = ? AND tbl_name = ? "
        "AND sql IS NOT NULL",
        (kind, table),
    ).fetchall()
    return [str(r["sql"]) for r in rows]


def _drop_triggers(conn: sqlite3.Connection, table: str) -> None:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?",
        (table,),
    ).fetchall()
    for row in rows:
        conn.execute(f"DROP TRIGGER {row['name']}")


def rebuild_table(
    conn: sqlite3.Connection,
    table: str,
    transform: Any,
    *,
    keep_triggers: bool = True,
) -> None:
    """Apply a DDL edit that SQLite cannot express as an ALTER.

    Follows the 12-step table rebuild from the SQLite docs: capture the
    dependent objects, create the replacement under a temporary name, copy the
    rows, swap, then restore indexes and triggers. This is exactly the shape of
    a real migration, which is why the gate must be able to see the result.
    """
    old_ddl = _table_ddl(conn, table)
    indexes = _objects_for(conn, table, "index")
    triggers = _objects_for(conn, table, "trigger")
    columns = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})")]

    new_ddl = transform(old_ddl).replace(table, f"{table}__new", 1)
    col_list = ", ".join(columns)

    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute("BEGIN")
        _drop_triggers(conn, table)
        conn.execute(new_ddl)
        conn.execute(
            f"INSERT INTO {table}__new ({col_list}) "
            f"SELECT {col_list} FROM {table}"  # nosec B608
        )
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE {table}__new RENAME TO {table}")
        for ddl in indexes:
            conn.execute(ddl)
        if keep_triggers:
            for ddl in triggers:
                conn.execute(ddl)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def mutate_drop_column(conn: sqlite3.Connection) -> None:
    """BREAKING — customer.phone disappears."""
    _drop_triggers(conn, "customer")
    conn.execute("ALTER TABLE customer DROP COLUMN phone")


def mutate_rename_column(conn: sqlite3.Connection) -> None:
    """BREAKING — customer.full_name becomes customer_name.

    Triggers are dropped first so SQLite does not silently rewrite them; the
    gate must see the physical rename exactly as a consumer would.
    """
    _drop_triggers(conn, "customer")
    conn.execute("ALTER TABLE customer RENAME COLUMN full_name TO customer_name")


def mutate_change_type(conn: sqlite3.Connection) -> None:
    """BREAKING — product.unit_price stops being a decimal.

    The type-dependent CHECK goes with it, exactly as it would in a real
    migration: you cannot keep ``ROUND(unit_price, 2) = unit_price`` on a TEXT
    column. That is part of why the change is breaking — the source loses a
    guarantee the warehouse was relying on.
    """
    rebuild_table(
        conn,
        "product",
        lambda ddl: re.sub(
            r"unit_price\s+DECIMAL\(12, 2\) NOT NULL\s+"
            r"CHECK \(unit_price >= 0 AND ROUND\(unit_price, 2\) = unit_price\)",
            "unit_price      TEXT NOT NULL",
            ddl,
        ),
    )


def mutate_nullable_to_not_null(conn: sqlite3.Connection) -> None:
    """BREAKING — product.discontinued_at becomes mandatory."""
    conn.execute(
        "UPDATE product SET discontinued_at = '2099-01-01T00:00:00.000Z' "
        "WHERE discontinued_at IS NULL"
    )
    rebuild_table(
        conn,
        "product",
        lambda ddl: ddl.replace(
            "discontinued_at TEXT,", "discontinued_at TEXT NOT NULL,"
        ),
    )


def mutate_remove_enum_value(conn: sqlite3.Connection) -> None:
    """BREAKING — 'CANCELLED' leaves the customer_order status domain."""
    conn.execute("DELETE FROM customer_order WHERE status = 'CANCELLED'")
    rebuild_table(
        conn,
        "customer_order",
        lambda ddl: re.sub(
            r"'PENDING',\s*'PAID',\s*'SHIPPED',\s*'DELIVERED',\s*'CANCELLED'",
            "'PENDING', 'PAID', 'SHIPPED', 'DELIVERED'",
            ddl,
        ),
    )


def mutate_add_nullable_column(conn: sqlite3.Connection) -> None:
    """COMPATIBLE — a new optional attribute appears."""
    conn.execute("ALTER TABLE customer ADD COLUMN loyalty_tier TEXT")


def mutate_add_enum_value(conn: sqlite3.Connection) -> None:
    """COMPATIBLE — the status domain gains a member."""
    rebuild_table(
        conn,
        "customer_order",
        lambda ddl: ddl.replace(
            "'DELIVERED', 'CANCELLED'", "'DELIVERED', 'CANCELLED', 'RETURNED'"
        ),
    )


def mutate_widen_type(conn: sqlite3.Connection) -> None:
    """COMPATIBLE — decimal precision increases."""
    rebuild_table(
        conn,
        "product",
        lambda ddl: ddl.replace("DECIMAL(12, 2)", "DECIMAL(18, 2)"),
    )


SCHEMA_MUTATIONS: dict[str, Any] = {
    "drop_column": mutate_drop_column,
    "rename_column": mutate_rename_column,
    "change_type": mutate_change_type,
    "nullable_to_not_null": mutate_nullable_to_not_null,
    "remove_enum_value": mutate_remove_enum_value,
    "add_nullable_column": mutate_add_nullable_column,
    "add_enum_value": mutate_add_enum_value,
    "widen_type": mutate_widen_type,
}

BREAKING_MUTATIONS = (
    "drop_column",
    "rename_column",
    "change_type",
    "nullable_to_not_null",
    "remove_enum_value",
)
COMPATIBLE_MUTATIONS = ("add_nullable_column", "add_enum_value", "widen_type")


def apply_mutation(conn: sqlite3.Connection, name: str) -> None:
    if name not in SCHEMA_MUTATIONS:
        raise KeyError(f"unknown mutation {name!r}")
    SCHEMA_MUTATIONS[name](conn)
