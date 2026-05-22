"""
    CDC capture layer — simulates WAL-based change data capture.

    Every insert/update/delete on the source tables produces a CDCRecord with a
    monotonically increasing sequence number. This pattern mirrors real production
    CDC tools like Debezium reading Postgres WAL logs.

    Replay safety: callers can checkpoint the last processed sequence and call
    records_since(offset) to replay only unprocessed changes after a restart.

    Production analogue: Debezium/Kafka connector reading Postgres WAL or similar
    database replication logs. The sequence number is equivalent to a Kafka offset
    or Postgres LSN (Log Sequence Number).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

VALID_OPERATIONS = frozenset({"insert", "update", "delete"})


@dataclass
class CDCRecord:
    """Represents a single change data capture event."""
    
    operation: str  # insert | update | delete
    table: str  # source table name
    primary_key: str  # PK value of the changed row
    data: dict[str, Any]  # full row snapshot at capture time
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0  # monotonic sequence number for replay

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(
                f"Invalid CDC operation {self.operation!r}. "
                f"Must be one of: {sorted(VALID_OPERATIONS)}"
            )


class CDCCapture:
    """
        In-process CDC log simulator.
        
        In production, this would be replaced by:
        - Debezium connectors reading database WAL
        - Kafka topics for distributed message passing
        - Cloud-native CDC services (GCP DataFlow, AWS DMS, etc.)
        
        For this assignment, we simulate the core CDC pattern:
        - Monotonic sequence numbers (like Kafka offsets or LSN)
        - Change history that can be replayed from any checkpoint
        - Support for insert/update/delete operations
    """

    def __init__(self, start_sequence: int = 0) -> None:
        self._log: list[CDCRecord] = []
        self._seq: int = start_sequence

    # ── public write API ─────────────────────────────────────────────────────

    def insert(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record an INSERT operation."""
        return self._record("insert", table, pk, data)

    def update(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record an UPDATE operation."""
        return self._record("update", table, pk, data)

    def delete(self, table: str, pk: str, data: dict[str, Any]) -> CDCRecord:
        """Record a DELETE operation. Data should be the pre-delete row state."""
        return self._record("delete", table, pk, data)

    # ── public read / replay API ─────────────────────────────────────────────

    def records_since(self, offset: int = 0) -> list[CDCRecord]:
        """
            Return all records with sequence > offset.
            
            This enables checkpoint-based replay: after processing sequence N,
            save N as a checkpoint. On restart, call records_since(N) to get only
            the unprocessed changes.
        """
        return [r for r in self._log if r.sequence > offset]

    @property
    def latest_sequence(self) -> int:
        """Return the highest sequence number seen so far."""
        return self._seq

    @property
    def log(self) -> list[CDCRecord]:
        """Return a copy of the entire log."""
        return list(self._log)

    # ── internal ─────────────────────────────────────────────────────────────

    def _record(
        self, operation: str, table: str, pk: str, data: dict[str, Any]
    ) -> CDCRecord:
        """Record a change and return the CDCRecord."""
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
