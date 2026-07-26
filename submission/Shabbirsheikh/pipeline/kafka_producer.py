"""
CDC transport: every source change is pushed straight to Kafka and we block
until the broker acknowledges it (acks="all") - that ack is the durability
point, same guarantee a WAL gives you, via a message broker instead.

Kafka's per-partition offset is the sequence number for ordering - the
topic has exactly one partition so offset order is true global order.
"""

import json
from datetime import datetime, timezone

from kafka import KafkaProducer

KNOWN_OPS = ("insert", "update", "delete")
TOPIC = "cdc-events-v4"
BOOTSTRAP_SERVERS = "192.168.1.11:9092"


def make_producer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",  # wait for full broker commit - this is our durability point
    )


class KafkaChangeCapture:
    """insert/update/delete interface for capturing a change, backed by Kafka."""

    def __init__(self, producer: KafkaProducer, topic: str = TOPIC):
        self.producer = producer
        self.topic = topic

    def emit(self, op: str, table: str, pk: str, row: dict) -> dict:
        if op not in KNOWN_OPS:
            raise ValueError(f"'{op}' is not a change op, expected one of {KNOWN_OPS}")

        payload = {
            "operation": op,
            "table_name": table,
            "primary_key": pk,
            # pre-serialize to a JSON string here (not a nested object) so the
            # Spark side can treat it as a plain string column and parse it
            # per-table later, same pattern the DuckDB lake used
            "data": json.dumps(row, default=str),
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        future = self.producer.send(self.topic, key=pk, value=payload)
        future.get(timeout=10)  # blocks until acked - turns "sent" into "durable"
        return payload

    def insert(self, table: str, pk: str, row: dict) -> dict:
        return self.emit("insert", table, pk, row)

    def update(self, table: str, pk: str, row: dict) -> dict:
        return self.emit("update", table, pk, row)

    def delete(self, table: str, pk: str, row: dict) -> dict:
        return self.emit("delete", table, pk, row)
