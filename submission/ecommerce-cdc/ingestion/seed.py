"""
Representative seed scenario shared by the demo, tests, and quality scripts.

The scenario exercises every CDC path: inserts across all five tables, an update
(order pending -> paid, payment pending -> settled), and a delete (customer c3).
Sequences are deterministic so time-travel assertions are stable:

    seq  1- 9  inserts (c1,c2,c3, p1,p2, o1, oi1,oi2, pay1)
    seq 10     update order o1 -> paid
    seq 11     update payment pay1 -> settled
    seq 12     delete customer c3
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from ingestion.cdc import CDCCapture

BASE_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
LATER_TS = BASE_TS + timedelta(hours=1)


def seed_capture() -> CDCCapture:
    cap = CDCCapture()

    cap.insert("customers", "c1", _customer("c1", "Alice", "alice@example.com"))
    cap.insert("customers", "c2", _customer("c2", "Bob", "bob@example.com"))
    cap.insert("customers", "c3", _customer("c3", "Carol", "carol@example.com"))

    cap.insert("products", "p1", _product("p1", "SKU-1", "Keyboard", 30.00))
    cap.insert("products", "p2", _product("p2", "SKU-2", "Mouse", 10.00))

    cap.insert("orders", "o1", _order("o1", "c1", "pending", 70.00))
    cap.insert("order_items", "oi1", _item("oi1", "o1", "p1", 2, 30.00, 60.00))
    cap.insert("order_items", "oi2", _item("oi2", "o1", "p2", 1, 10.00, 10.00))
    cap.insert("payments", "pay1", _payment("pay1", "o1", 70.00, "card", "pending"))

    cap.update(
        "orders",
        "o1",
        _order("o1", "c1", "paid", 70.00, updated_at=LATER_TS),
        before=_order("o1", "c1", "pending", 70.00),
    )
    cap.update(
        "payments",
        "pay1",
        _payment("pay1", "o1", 70.00, "card", "settled", settled_at=LATER_TS),
        before=_payment("pay1", "o1", 70.00, "card", "pending"),
    )
    cap.delete("customers", "c3", before=_customer("c3", "Carol", "carol@example.com"))

    return cap


def _customer(cid: str, name: str, email: str) -> dict:
    return {
        "customer_id": cid,
        "name": name,
        "email": email,
        "status": "active",
        "created_at": BASE_TS,
        "updated_at": BASE_TS,
    }


def _product(pid: str, sku: str, name: str, price: float) -> dict:
    return {
        "product_id": pid,
        "sku": sku,
        "name": name,
        "price": price,
        "status": "active",
        "created_at": BASE_TS,
        "updated_at": BASE_TS,
    }


def _order(
    oid: str, cid: str, status: str, total: float, updated_at: datetime = BASE_TS
) -> dict:
    return {
        "order_id": oid,
        "customer_id": cid,
        "status": status,
        "total": total,
        "currency": "USD",
        "created_at": BASE_TS,
        "updated_at": updated_at,
    }


def _item(iid: str, oid: str, pid: str, qty: int, unit: float, line: float) -> dict:
    return {
        "order_item_id": iid,
        "order_id": oid,
        "product_id": pid,
        "quantity": qty,
        "unit_price": unit,
        "line_total": line,
    }


def _payment(
    pid: str,
    oid: str,
    amount: float,
    method: str,
    status: str,
    settled_at: Optional[datetime] = None,
) -> dict:
    return {
        "payment_id": pid,
        "order_id": oid,
        "amount": amount,
        "method": method,
        "status": status,
        "created_at": BASE_TS,
        "settled_at": settled_at,
    }
