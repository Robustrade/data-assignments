"""
The warehouse tables are typed loosely (see warehouse.py) so the merge logic
stays generic - none of the source's constraints (NOT NULL, CHECK, FK)
exist on the warehouse side. This script puts that parity back: every rule
the source enforces gets re-checked here, so a CDC bug or an out-of-order
write can't silently corrupt the warehouse unnoticed.

System checks: PK uniqueness, not-null, referential integrity, enums.
Business checks: non-negative amounts, order total vs line items, payment
timing, and (needs the lake's full history, not just latest state) status
transitions that shouldn't be possible.
"""

import json
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LAKE_PATH = PROJECT_ROOT / "data" / "lake_parquet"
WAREHOUSE_DB = PROJECT_ROOT / "data" / "warehouse.duckdb"

# only forward transitions are legal; anything not listed here is a violation
ALLOWED_ORDER_TRANSITIONS = {
    "pending": {"paid", "cancelled"},
    "paid": {"shipped", "refunded"},
    "shipped": {"refunded"},
    "cancelled": set(),
    "refunded": set(),
}
ALLOWED_PAYMENT_TRANSITIONS = {
    "pending": {"settled", "failed"},
    "settled": {"refunded"},
    "failed": set(),
    "refunded": set(),
}


class DataQualityViolation(Exception):
    pass


# ---------------------------------------------------------------- system --

def check_pk_uniqueness(conn: duckdb.DuckDBPyConnection, table: str, pk: str) -> list[str]:
    dupes = conn.execute(
        f"SELECT {pk}, COUNT(*) c FROM {table} GROUP BY {pk} HAVING COUNT(*) > 1"
    ).fetchall()
    return [f"{table}.{pk}={row[0]}: duplicated {row[1]} times" for row in dupes]


def check_not_null(conn: duckdb.DuckDBPyConnection, table: str, columns: list[str]) -> list[str]:
    problems = []
    for col in columns:
        n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
        if n:
            problems.append(f"{table}.{col}: {n} row(s) with NULL where a value is required")
    return problems


def check_referential_integrity(
    conn: duckdb.DuckDBPyConnection, child_table: str, fk_col: str, parent_table: str, parent_pk: str
) -> list[str]:
    orphans = conn.execute(f"""
        SELECT c.{fk_col} FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{fk_col} = p.{parent_pk}
        WHERE p.{parent_pk} IS NULL
    """).fetchall()
    return [f"{child_table}.{fk_col}={row[0]}: no matching {parent_table}.{parent_pk}" for row in orphans]


def check_enum_domain(conn: duckdb.DuckDBPyConnection, table: str, col: str, allowed: set[str]) -> list[str]:
    placeholders = ", ".join(f"'{v}'" for v in allowed)
    bad = conn.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} NOT IN ({placeholders})").fetchall()
    return [f"{table}.{col}: unexpected value {row[0]!r}, allowed are {sorted(allowed)}" for row in bad]


# -------------------------------------------------------------- business --

def check_non_negative_amounts(conn: duckdb.DuckDBPyConnection) -> list[str]:
    problems = []
    bad_orders = conn.execute("SELECT order_id FROM wh_orders WHERE total_amount < 0").fetchall()
    problems += [f"wh_orders.total_amount negative for {r[0]}" for r in bad_orders]

    bad_items = conn.execute(
        "SELECT order_item_id FROM wh_order_items WHERE unit_price < 0 OR quantity <= 0"
    ).fetchall()
    problems += [f"wh_order_items bad quantity/unit_price for {r[0]}" for r in bad_items]

    bad_payments = conn.execute("SELECT payment_id FROM wh_payments WHERE amount <= 0").fetchall()
    problems += [f"wh_payments.amount not positive for {r[0]}" for r in bad_payments]
    return problems


def check_order_totals_match_items(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute("""
        SELECT o.order_id, o.total_amount, COALESCE(SUM(i.quantity * i.unit_price), 0) AS items_total
        FROM wh_orders o
        LEFT JOIN wh_order_items i ON i.order_id = o.order_id AND i._deleted = FALSE
        WHERE o._deleted = FALSE
        GROUP BY o.order_id, o.total_amount
        HAVING ROUND(o.total_amount, 2) != ROUND(items_total, 2)
    """).fetchall()
    return [f"order {r[0]}: total_amount={r[1]} but line items sum to {r[2]}" for r in rows]


def check_payment_timing(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute("""
        SELECT payment_id FROM wh_payments
        WHERE paid_at IS NOT NULL AND paid_at < created_at
    """).fetchall()
    return [f"wh_payments.{r[0]}: paid_at is before created_at" for r in rows]


def _status_history_per_entity(lake_events: list[dict], table: str) -> dict[str, list[str]]:
    history: dict[str, list[str]] = {}
    for event in sorted(lake_events, key=lambda e: e["sequence"]):
        if event["table_name"] != table or event["operation"] == "delete":
            continue
        data = json.loads(event["data"]) if isinstance(event["data"], str) else event["data"]
        status = data.get("status")
        if status is not None:
            history.setdefault(event["primary_key"], []).append(status)
    return history


def check_status_transitions(lake_events: list[dict], table: str, allowed: dict[str, set[str]]) -> list[str]:
    problems = []
    for pk, statuses in _status_history_per_entity(lake_events, table).items():
        for before, after in zip(statuses, statuses[1:]):
            if before == after:
                continue  # a re-emitted same-status update isn't a transition
            if after not in allowed.get(before, set()):
                problems.append(f"{table} {pk}: illegal status transition {before} -> {after}")
    return problems


# ------------------------------------------------------------------ main --

def run_all(wh_conn: duckdb.DuckDBPyConnection, lake_events: list[dict]) -> dict[str, list[str]]:
    results = {
        "pk_uniqueness": (
            check_pk_uniqueness(wh_conn, "wh_customers", "customer_id")
            + check_pk_uniqueness(wh_conn, "wh_orders", "order_id")
            + check_pk_uniqueness(wh_conn, "wh_order_items", "order_item_id")
            + check_pk_uniqueness(wh_conn, "wh_payments", "payment_id")
        ),
        "not_null": (
            check_not_null(wh_conn, "wh_customers", ["name", "email", "status"])
            + check_not_null(wh_conn, "wh_orders", ["customer_id", "status", "total_amount"])
            + check_not_null(wh_conn, "wh_payments", ["amount", "status"])
        ),
        "referential_integrity": (
            check_referential_integrity(wh_conn, "wh_orders", "customer_id", "wh_customers", "customer_id")
            + check_referential_integrity(wh_conn, "wh_order_items", "order_id", "wh_orders", "order_id")
            + check_referential_integrity(wh_conn, "wh_payments", "order_id", "wh_orders", "order_id")
        ),
        "enum_domain": (
            check_enum_domain(wh_conn, "wh_customers", "status", {"active", "suspended", "closed"})
            + check_enum_domain(wh_conn, "wh_orders", "status", {"pending", "paid", "shipped", "cancelled", "refunded"})
            + check_enum_domain(wh_conn, "wh_payments", "status", {"pending", "settled", "failed", "refunded"})
        ),
        "non_negative_amounts": check_non_negative_amounts(wh_conn),
        "order_totals_match_items": check_order_totals_match_items(wh_conn),
        "payment_timing": check_payment_timing(wh_conn),
        "order_status_transitions": check_status_transitions(lake_events, "orders", ALLOWED_ORDER_TRANSITIONS),
        "payment_status_transitions": check_status_transitions(lake_events, "payments", ALLOWED_PAYMENT_TRANSITIONS),
    }
    return {k: v for k, v in results.items() if v}


def enforce(wh_conn: duckdb.DuckDBPyConnection, lake_events: list[dict]) -> None:
    failures = run_all(wh_conn, lake_events)
    if failures:
        lines = [f"[{category}] {p}" for category, problems in failures.items() for p in problems]
        raise DataQualityViolation("Data quality checks failed:\n" + "\n".join(f"  - {l}" for l in lines))


def _read_lake_events(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Reads every event out of the Parquet lake, reusing the warehouse
    connection since read_parquet() works from any open connection."""
    rows = conn.execute(
        f"SELECT * FROM read_parquet('{LAKE_PATH.as_posix()}/*/*.parquet') ORDER BY sequence"
    ).fetchall()
    cols = [d[0] for d in conn.description]
    return [dict(zip(cols, row)) for row in rows]


if __name__ == "__main__":
    if not WAREHOUSE_DB.exists() or not LAKE_PATH.exists():
        print(f"No warehouse/lake found at {WAREHOUSE_DB} / {LAKE_PATH} - nothing to check yet.")
        sys.exit(0)

    wh_conn = duckdb.connect(str(WAREHOUSE_DB))
    lake_events = _read_lake_events(wh_conn)

    try:
        enforce(wh_conn, lake_events)
    except DataQualityViolation as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    print("All data quality checks passed.")
