"""
CDC capture layer.

Simulates WAL-based change capture: every insert/update/delete on the source
produces a CDCRecord with a monotonically increasing sequence number.

Replay safety: callers can checkpoint the last processed sequence and call
records_since(offset) to replay only unprocessed changes after a restart.
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
    In-process CDC log.

    Production analogue: a Debezium/Kafka connector reading Postgres WAL.
    Each record carries a sequence number equivalent to a Kafka offset or
    Postgres LSN for checkpoint-based replay.
    """

    def __init__(self) -> None:
        self._log: list[CDCRecord] = []
        self._seq: int = 0

    # ── public write API ─────────────────────────────────────────────────────

    def insert(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("insert", table, pk, data)

    def update(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("update", table, pk, data)

    def delete(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        return self._record("delete", table, pk, data)

    # ── public read / replay API ─────────────────────────────────────────────

    def records_since(self, offset: int = 0) -> list[CDCRecord]:
        """Return all records with sequence > offset (checkpoint replay)."""
        return [r for r in self._log if r.sequence > offset]

    @property
    def latest_sequence(self) -> int:
        return self._seq

    @property
    def log(self) -> list[CDCRecord]:
        return list(self._log)

    # ── internal ─────────────────────────────────────────────────────────────

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
