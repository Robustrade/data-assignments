"""
Debezium/Kafka-style CDC event model.

The production pipeline uses Debezium on PostgreSQL WAL and Kafka topics. This
module is a deterministic local analogue used by tests and scripts: it carries
Kafka offsets, source LSNs, operation codes, before/after images, and stable
event ids for replay-safe downstream writes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

VALID_OPS = frozenset({"c", "u", "d", "r"})


@dataclass(frozen=True)
class CDCEvent:
    topic: str
    table: str
    primary_key: str
    op: str
    after: dict[str, Any] | None
    before: dict[str, Any] | None = None
    source_lsn: int = 0
    tx_id: str = "local-tx"
    kafka_partition: int = 0
    kafka_offset: int = 0
    schema_id: str = "local-v1"
    source_event_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if self.op not in VALID_OPS:
            raise ValueError(f"Invalid Debezium operation {self.op!r}")
        if self.op != "d" and self.after is None:
            raise ValueError("Create, update, and snapshot events require an after image")
        if self.op == "d" and self.before is None:
            raise ValueError("Delete events require a before image")

    @property
    def event_id(self) -> str:
        identity = {
            "table": self.table,
            "pk": self.primary_key,
            "lsn": self.source_lsn,
            "tx_id": self.tx_id,
            "op": self.op,
        }
        return hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode("utf-8")
        ).hexdigest()

    @property
    def current_image(self) -> dict[str, Any]:
        if self.op == "d":
            return self.before or {}
        return self.after or {}


class KafkaCDCLog:
    """Small in-memory Kafka topic analogue with monotonically increasing offsets."""

    def __init__(self, topic_prefix: str = "ticketing.public") -> None:
        self.topic_prefix = topic_prefix
        self._events: list[CDCEvent] = []
        self._offset = 0
        self._lsn = 10_000

    def create(self, table: str, pk: str, after: dict[str, Any]) -> CDCEvent:
        return self._append(table, pk, "c", after=after, before=None)

    def update(
        self, table: str, pk: str, before: dict[str, Any], after: dict[str, Any]
    ) -> CDCEvent:
        return self._append(table, pk, "u", after=after, before=before)

    def delete(self, table: str, pk: str, before: dict[str, Any]) -> CDCEvent:
        return self._append(table, pk, "d", after=None, before=before)

    def events_after(self, offset: int = -1) -> list[CDCEvent]:
        return [event for event in self._events if event.kafka_offset > offset]

    @property
    def latest_offset(self) -> int:
        return self._offset - 1

    def _append(
        self,
        table: str,
        pk: str,
        op: str,
        after: dict[str, Any] | None,
        before: dict[str, Any] | None,
    ) -> CDCEvent:
        event = CDCEvent(
            topic=f"{self.topic_prefix}.{table}",
            table=table,
            primary_key=pk,
            op=op,
            after=after,
            before=before,
            source_lsn=self._lsn,
            tx_id=f"tx-{self._lsn}",
            kafka_offset=self._offset,
        )
        self._events.append(event)
        self._offset += 1
        self._lsn += 1
        return event

