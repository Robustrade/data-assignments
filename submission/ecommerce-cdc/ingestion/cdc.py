"""
CDC capture layer.

Simulates WAL/logical-replication change capture: every insert/update/delete on the
source produces a ``CDCRecord`` with a monotonically increasing ``sequence`` (the
local analogue of a Kafka offset or Postgres LSN) plus before/after row images.

Replay safety: a consumer checkpoints the last processed ``sequence`` and calls
``records_since(offset)`` to replay only unprocessed changes after a restart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

VALID_OPERATIONS = frozenset({"insert", "update", "delete"})


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CDCRecord:
    operation: str
    table: str
    primary_key: str
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    captured_at: datetime = field(default_factory=_utcnow)
    sequence: int = 0

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(
                f"Invalid CDC operation {self.operation!r}. "
                f"Must be one of: {sorted(VALID_OPERATIONS)}"
            )

    @property
    def image(self) -> Dict[str, Any]:
        """Row image to persist: ``after`` for insert/update, ``before`` for delete."""
        return (self.after if self.after is not None else self.before) or {}


class CDCCapture:
    """
    In-process CDC log.

    Production analogue: a Debezium/Kafka connector reading the Postgres WAL. Each
    record carries a ``sequence`` equivalent to a Kafka offset / Postgres LSN used for
    checkpoint-based replay.
    """

    def __init__(self, start_sequence: int = 0) -> None:
        self._log: List[CDCRecord] = []
        self._seq: int = start_sequence

    # -- write API ------------------------------------------------------------

    def insert(self, table: str, pk: str, after: Dict[str, Any]) -> CDCRecord:
        return self._record("insert", table, pk, None, after)

    def update(
        self,
        table: str,
        pk: str,
        after: Dict[str, Any],
        before: Optional[Dict[str, Any]] = None,
    ) -> CDCRecord:
        return self._record("update", table, pk, before, after)

    def delete(
        self,
        table: str,
        pk: str,
        before: Optional[Dict[str, Any]] = None,
    ) -> CDCRecord:
        return self._record("delete", table, pk, before, None)

    # -- read / replay API ----------------------------------------------------

    def records_since(self, offset: int = 0) -> List[CDCRecord]:
        """Return all records with ``sequence`` > offset (checkpoint replay)."""
        return [r for r in self._log if r.sequence > offset]

    @property
    def latest_sequence(self) -> int:
        return self._seq

    @property
    def log(self) -> List[CDCRecord]:
        return list(self._log)

    # -- internal -------------------------------------------------------------

    def _record(
        self,
        operation: str,
        table: str,
        pk: str,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
    ) -> CDCRecord:
        self._seq += 1
        rec = CDCRecord(
            operation=operation,
            table=table,
            primary_key=pk,
            before=before,
            after=after,
            sequence=self._seq,
        )
        self._log.append(rec)
        return rec
