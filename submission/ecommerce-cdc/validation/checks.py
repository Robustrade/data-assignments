"""
Validation parity layer.

Re-asserts the source's system and business rules against the warehouse outputs so
downstream consumers are protected even when the source is wrong. Status-transition
legality is validated from the ordered lake history (the durable record of change).

Each function returns a list of human-readable failure messages; an empty list means
the warehouse is consistent with the source contract.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

import duckdb

from lake.lake import read_events
from source.models import (
    ORDER_STATUS_TRANSITIONS,
    PAYMENT_STATUS_TRANSITIONS,
    SCHEMA_CONTRACT,
)
from warehouse.warehouse import current_table


def run_system_checks(conn: duckdb.DuckDBPyConnection) -> List[str]:
    failures: List[str] = []

    for table, contract in SCHEMA_CONTRACT.items():
        wh = current_table(table)
        pk = contract.primary_key

        total = conn.execute(f"SELECT COUNT(*) FROM {wh}").fetchone()[0]
        distinct = conn.execute(f"SELECT COUNT(DISTINCT {pk}) FROM {wh}").fetchone()[0]
        if total != distinct:
            failures.append(
                f"{wh}: PK not unique ({total} rows, {distinct} distinct {pk})"
            )

        for col in contract.columns:
            if col.nullable:
                continue
            nulls = conn.execute(
                f"SELECT COUNT(*) FROM {wh} WHERE NOT _deleted AND {col.name} IS NULL"
            ).fetchone()[0]
            if nulls:
                failures.append(f"{wh}.{col.name}: {nulls} NULL value(s)")

        for col_name, domain in contract.enum_domains.items():
            allowed = ", ".join(f"'{v}'" for v in domain)
            bad = conn.execute(
                f"SELECT COUNT(*) FROM {wh} "
                f"WHERE NOT _deleted AND {col_name} NOT IN ({allowed})"
            ).fetchone()[0]
            if bad:
                failures.append(f"{wh}.{col_name}: {bad} row(s) outside enum domain")

        for fk_col, (parent, parent_col) in contract.foreign_keys.items():
            parent_wh = current_table(parent)
            orphans = conn.execute(
                f"""
                SELECT COUNT(*) FROM {wh} c
                WHERE NOT c._deleted AND c.{fk_col} IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {parent_wh} p
                    WHERE p.{parent_col} = c.{fk_col} AND NOT p._deleted
                  )
                """
            ).fetchone()[0]
            if orphans:
                failures.append(
                    f"{wh}.{fk_col}: {orphans} orphan(s) missing {parent_wh}.{parent_col}"
                )

    return failures


def run_business_checks(conn: duckdb.DuckDBPyConnection) -> List[str]:
    failures: List[str] = []

    bad_line = conn.execute(
        "SELECT COUNT(*) FROM wh_order_items "
        "WHERE NOT _deleted AND line_total <> quantity * unit_price"
    ).fetchone()[0]
    if bad_line:
        failures.append(
            f"wh_order_items: {bad_line} row(s) where line_total != quantity*unit_price"
        )

    mismatched = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT o.order_id, o.total,
                   COALESCE(SUM(i.line_total), 0) AS items_total
            FROM wh_orders o
            LEFT JOIN wh_order_items i
              ON i.order_id = o.order_id AND NOT i._deleted
            WHERE NOT o._deleted
            GROUP BY o.order_id, o.total
            HAVING o.total <> COALESCE(SUM(i.line_total), 0)
        )
        """
    ).fetchone()[0]
    if mismatched:
        failures.append(
            f"wh_orders: {mismatched} order(s) where total != sum(order_items.line_total)"
        )

    neg_amount = conn.execute(
        "SELECT COUNT(*) FROM wh_payments WHERE NOT _deleted AND amount < 0"
    ).fetchone()[0]
    if neg_amount:
        failures.append(f"wh_payments: {neg_amount} row(s) with negative amount")

    bad_price = conn.execute(
        "SELECT COUNT(*) FROM wh_products WHERE NOT _deleted AND price <= 0"
    ).fetchone()[0]
    if bad_price:
        failures.append(f"wh_products: {bad_price} row(s) with non-positive price")

    bad_qty = conn.execute(
        "SELECT COUNT(*) FROM wh_order_items WHERE NOT _deleted AND quantity <= 0"
    ).fetchone()[0]
    if bad_qty:
        failures.append(f"wh_order_items: {bad_qty} row(s) with non-positive quantity")

    bad_settle = conn.execute(
        "SELECT COUNT(*) FROM wh_payments "
        "WHERE NOT _deleted AND settled_at IS NOT NULL AND settled_at < created_at"
    ).fetchone()[0]
    if bad_settle:
        failures.append(
            f"wh_payments: {bad_settle} row(s) where settled_at < created_at"
        )

    failures.extend(
        _status_transition_failures(conn, "orders", ORDER_STATUS_TRANSITIONS)
    )
    failures.extend(
        _status_transition_failures(conn, "payments", PAYMENT_STATUS_TRANSITIONS)
    )
    return failures


def run_all_checks(conn: duckdb.DuckDBPyConnection) -> List[str]:
    return run_system_checks(conn) + run_business_checks(conn)


def _status_transition_failures(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    transitions: Dict[str, tuple],
) -> List[str]:
    failures: List[str] = []
    histories: Dict[str, List[str]] = defaultdict(list)

    for event in read_events(conn):
        if event["table_name"] != table or event["operation"] == "delete":
            continue
        status = event["data"].get("status")
        if status is not None:
            histories[event["primary_key"]].append(status)

    for pk, statuses in histories.items():
        for prev, curr in zip(statuses, statuses[1:]):
            if prev == curr:
                continue
            if curr not in transitions.get(prev, ()):  # illegal transition
                failures.append(
                    f"{table} {pk}: illegal status transition {prev} -> {curr}"
                )
    return failures
