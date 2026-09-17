import hashlib
from datetime import datetime, timezone


def event_id(table, operation, pk, seq):
    raw = f"{table}|{operation}|{pk}|{seq}".encode()
    return hashlib.sha256(raw).hexdigest()


def make_event(table, operation, pk, row, seq, schema_version="1", changed_at=None):
    return {
        "event_id": event_id(table, operation, pk, seq),
        "table_name": table,
        "operation": operation,
        "primary_key": pk,
        "row_data": row,
        "changed_at": changed_at or datetime.now(timezone.utc).isoformat(),
        "source_lsn": seq,
        "schema_version": schema_version,
    }


def build_demo_events():
    # Local CDC simulation. Production would consume WAL/binlog events.
    ts = {
        1: "2026-09-17T09:00:00+00:00",
        2: "2026-09-17T09:30:00+00:00",
        3: "2026-09-17T09:35:00+00:00",
        4: "2026-09-17T09:40:00+00:00",
        5: "2026-09-17T10:00:00+00:00",
        6: "2026-09-17T10:01:00+00:00",
        7: "2026-09-17T10:02:00+00:00",
        8: "2026-09-17T10:03:00+00:00",
        9: "2026-09-17T10:05:00+00:00",
    }

    return [
        # Customer initial state.
        make_event(
            "customers",
            "INSERT",
            {"customer_id": 1},
            {
                "customer_id": 1,
                "email": "alice@example.com",
                "full_name": "Alice",
                "status": "ACTIVE",
                "created_at": ts[1],
                "updated_at": ts[1],
            },
            1,
            changed_at=ts[1],
        ),

        # Product used by the order.
        make_event(
            "products",
            "INSERT",
            {"product_id": 102},
            {
                "product_id": 102,
                "sku": "SKU-102",
                "name": "Data Platform License",
                "unit_price": 75.0,
                "currency": "USD",
                "status": "ACTIVE",
                "created_at": ts[2],
                "updated_at": ts[2],
            },
            2,
            changed_at=ts[2],
        ),

        # Order starts as pending.
        make_event(
            "orders",
            "INSERT",
            {"order_id": 1001},
            {
                "order_id": 1001,
                "customer_id": 1,
                "status": "PENDING",
                "order_total": 75.0,
                "currency": "USD",
                "ordered_at": ts[3],
                "updated_at": ts[3],
            },
            3,
            changed_at=ts[3],
        ),

        # Order line.
        make_event(
            "order_items",
            "INSERT",
            {"order_id": 1001, "line_number": 1},
            {
                "order_id": 1001,
                "line_number": 1,
                "product_id": 102,
                "quantity": 1,
                "unit_price": 75.0,
            },
            4,
            changed_at=ts[4],
        ),

        # Customer update used by the SCD2 test.
        make_event(
            "customers",
            "UPDATE",
            {"customer_id": 1},
            {
                "customer_id": 1,
                "email": "alice@example.com",
                "full_name": "Alice P",
                "status": "ACTIVE",
                "created_at": ts[1],
                "updated_at": ts[5],
            },
            5,
            changed_at=ts[5],
        ),

        # Order becomes paid.
        make_event(
            "orders",
            "UPDATE",
            {"order_id": 1001},
            {
                "order_id": 1001,
                "customer_id": 1,
                "status": "PAID",
                "order_total": 75.0,
                "currency": "USD",
                "ordered_at": ts[3],
                "updated_at": ts[6],
            },
            6,
            changed_at=ts[6],
        ),

        # Payment captured.
        make_event(
            "payments",
            "INSERT",
            {"payment_id": 5001},
            {
                "payment_id": 5001,
                "order_id": 1001,
                "amount": 75.0,
                "status": "CAPTURED",
                "paid_at": ts[7],
                "created_at": ts[7],
            },
            7,
            changed_at=ts[7],
        ),

        # Delete product to exercise DELETE CDC handling.
        make_event(
            "products",
            "DELETE",
            {"product_id": 102},
            {},
            8,
            changed_at=ts[8],
        ),
    ]