"""
CDC capture layer.

Simulates WAL-based or log-based change data capture: every insert, update,
or delete on the source produces a CDCRecord with a monotonically increasing
sequence number (equivalent to a Kafka offset or Postgres LSN).

Replay safety: consumers can store the last processed sequence number as a
checkpoint and call `records_since(offset)` to replay changes cleanly after restart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

VALID_OPERATIONS = frozenset({"insert", "update", "delete"})


@dataclass
class CDCRecord:
    operation: str
    table: str
    primary_key: str
    data: dict[str, Any]
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(
                f"Invalid CDC operation {self.operation!r}. "
                f"Must be one of: {sorted(VALID_OPERATIONS)}"
            )


class CDCCapture:
    """
    In-memory CDC log representing an upstream event stream / WAL feed.

    Production analogue: Debezium reading PostgreSQL WAL or MySQL binlog,
    publishing change events to Apache Kafka or AWS Kinesis.
    """

    def __init__(self) -> None:
        self._log: list[CDCRecord] = []
        self._seq: int = 0

    def insert(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record an INSERT event."""
        return self._record("insert", table, pk, data)

    def update(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record an UPDATE event."""
        return self._record("update", table, pk, data)

    def delete(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record a DELETE event."""
        return self._record("delete", table, pk, data)

    def records_since(self, offset: int = 0) -> list[CDCRecord]:
        """Return all CDC records with sequence > offset for checkpoint replay."""
        return [r for r in self._log if r.sequence > offset]

    @property
    def latest_sequence(self) -> int:
        """Return highest sequence number produced so far."""
        return self._seq

    @property
    def log(self) -> list[CDCRecord]:
        """Return copy of the full CDC log."""
        return list(self._log)

    def _record(
        self, operation: str, table: str, pk: str, data: dict[str, Any]
    ) -> CDCRecord:
        self._seq += 1
        rec = CDCRecord(
            operation=operation,
            table=table,
            primary_key=pk,
            data=data,
            sequence=self._seq,
        )
        self._log.append(rec)
        return rec
