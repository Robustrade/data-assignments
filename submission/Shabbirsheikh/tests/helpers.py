"""Small row builders shared across test files, just to keep test bodies short."""

from datetime import datetime, timezone
from decimal import Decimal


class EventBuilder:
    """Test-only: builds a sequence-numbered event list in the shape
    apply_lake_events() expects. Production uses Kafka's own offset as the
    sequence instead (see pipeline.kafka_producer.KafkaChangeCapture)."""

    def __init__(self):
        self.events: list[dict] = []
        self._next_seq = 1

    def insert(self, table: str, pk: str, row: dict) -> dict:
        return self._emit("insert", table, pk, row)

    def update(self, table: str, pk: str, row: dict) -> dict:
        return self._emit("update", table, pk, row)

    def delete(self, table: str, pk: str, row: dict) -> dict:
        return self._emit("delete", table, pk, row)

    def _emit(self, operation: str, table: str, pk: str, row: dict) -> dict:
        event = {
            "sequence": self._next_seq,
            "operation": operation,
            "table_name": table,
            "primary_key": pk,
            "data": row,
            "captured_at": datetime.now(timezone.utc),
        }
        self.events.append(event)
        self._next_seq += 1
        return event


def customer_row(customer_id="c1", status="active", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "customer_id": customer_id, "name": "Test User", "email": f"{customer_id}@example.com",
        "status": status, "created_at": now, "updated_at": now,
    }
    row.update(overrides)
    return row


def order_row(order_id="o1", customer_id="c1", status="pending", total_amount="10.00", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "order_id": order_id, "customer_id": customer_id, "status": status,
        "total_amount": Decimal(total_amount), "discount_code": None,
        "created_at": now, "updated_at": now,
    }
    row.update(overrides)
    return row


def order_item_row(order_item_id="i1", order_id="o1", quantity=1, unit_price="10.00", **overrides):
    row = {
        "order_item_id": order_item_id, "order_id": order_id, "sku": "SKU1",
        "quantity": quantity, "unit_price": Decimal(unit_price),
    }
    row.update(overrides)
    return row


def payment_row(payment_id="p1", order_id="o1", amount="10.00", status="pending", paid_at=None, **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "payment_id": payment_id, "order_id": order_id, "amount": Decimal(amount),
        "status": status, "created_at": now, "paid_at": paid_at,
    }
    row.update(overrides)
    return row
