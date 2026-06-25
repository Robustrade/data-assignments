"""
CDC capture layer.

What is captured
----------------
Every insert, update and delete on the source produces a CDCRecord with:
  - sequence    : monotonically increasing, never reused
  - operation   : 'insert' | 'update' | 'delete'
  - table       : name of the source table
  - primary_key : the source row's PK value
  - before      : row image *before* the change (None for inserts)
  - after       : row image *after*  the change (None for deletes)
  - captured_at : UTC timestamp at capture time

`before` and `after` together let downstream consumers run *before/after*
diffs (useful for SCD2 history, audit, and DLQ inspection).

Production analogue
-------------------
A real Postgres source would hand us this same envelope from logical
replication / Debezium: the LSN ↔ `sequence`, the relational change
record ↔ `before`/`after`, and the commit timestamp ↔ `captured_at`.
The in-process `CDCCapture` class plays the role of the WAL plus the
connector.

Replay & restart
----------------
The pipeline checkpoints the **last sequence successfully applied** in
the warehouse (`pipeline.checkpoint`). After a restart we ask the
capture log for `records_since(checkpoint)` and reapply those — at-least
once delivery, but the warehouse merge is **idempotent by sequence**
(see `pipeline.warehouse`) so duplicates do not corrupt state.

Out-of-order arrival
--------------------
Records are appended to the log in capture order, so within a single
log they are in order. Across distributed sources, the warehouse keeps
`_cdc_seq` per row and ignores any incoming event with a sequence
**less than or equal** to what is already applied — late events do not
clobber newer state.

Schema-aware capture
--------------------
`CDCCapture` is created with a reference to a `SchemaDetector`. When
`require_compatible_schema=True`, calling any of the write methods on a
table with a broken contract raises `SchemaContractViolation` and emits
no records. This is the stop-the-line hook that satisfies the
"fail closed" requirement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


VALID_OPERATIONS = frozenset({"insert", "update", "delete"})


class SchemaContractViolation(RuntimeError):
    """Raised when CDC capture is attempted against a non-compliant source."""


@dataclass
class CDCRecord:
    operation: str
    table: str
    primary_key: str
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(
                f"Invalid CDC operation {self.operation!r}. "
                f"Must be one of: {sorted(VALID_OPERATIONS)}"
            )
        if self.operation == "insert" and self.after is None:
            raise ValueError("insert events must carry an 'after' image")
        if self.operation == "delete" and self.before is None:
            raise ValueError("delete events must carry a 'before' image")
        if self.operation == "update" and (
            self.before is None or self.after is None
        ):
            raise ValueError("update events must carry both 'before' and 'after'")

    @property
    def data(self) -> dict[str, Any]:
        """
        Convenience accessor for the row image used by the warehouse merge:
        for inserts/updates this is the post-image; for deletes it is the
        pre-image (so we still know the row's state at delete time).
        """
        return self.after if self.after is not None else (self.before or {})


class CDCCapture:
    """
    In-process change log used by tests and the demo orchestrator.

    Production analogue: a Debezium / pglogical connector reading the
    Postgres WAL and writing each event to a Kafka topic. Each record's
    `sequence` would be a Kafka offset or a Postgres LSN.
    """

    def __init__(
        self,
        schema_detector: Optional["object"] = None,  # SchemaDetector
        *,
        require_compatible_schema: bool = False,
    ) -> None:
        self._log: list[CDCRecord] = []
        self._seq: int = 0
        self._schema_detector = schema_detector
        self._require_compatible_schema = require_compatible_schema

    # ── public write API ─────────────────────────────────────────────────────

    def insert(
        self,
        table: str,
        pk: str,
        after: dict[str, Any],
        *,
        captured_at: Optional[datetime] = None,
    ) -> CDCRecord:
        self._enforce_schema(table)
        return self._record(
            "insert", table, pk, before=None, after=after,
            captured_at=captured_at,
        )

    def update(
        self,
        table: str,
        pk: str,
        before: dict[str, Any],
        after: dict[str, Any],
        *,
        captured_at: Optional[datetime] = None,
    ) -> CDCRecord:
        self._enforce_schema(table)
        return self._record(
            "update", table, pk, before=before, after=after,
            captured_at=captured_at,
        )

    def delete(
        self,
        table: str,
        pk: str,
        before: dict[str, Any],
        *,
        captured_at: Optional[datetime] = None,
    ) -> CDCRecord:
        self._enforce_schema(table)
        return self._record(
            "delete", table, pk, before=before, after=None,
            captured_at=captured_at,
        )

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

    def _enforce_schema(self, table: str) -> None:
        if not self._require_compatible_schema:
            return
        detector = self._schema_detector
        if detector is None:
            return
        report = detector.check_table(table)
        if report.has_breaking():
            raise SchemaContractViolation(
                f"Schema contract violated for {table!r}: "
                + "; ".join(d.message for d in report.breaking)
            )

    def _record(
        self,
        operation: str,
        table: str,
        pk: str,
        *,
        before: Optional[dict[str, Any]],
        after: Optional[dict[str, Any]],
        captured_at: Optional[datetime] = None,
    ) -> CDCRecord:
        self._seq += 1
        kwargs: dict[str, Any] = {
            "operation": operation,
            "table": table,
            "primary_key": pk,
            "before": before,
            "after": after,
            "sequence": self._seq,
        }
        if captured_at is not None:
            kwargs["captured_at"] = captured_at
        rec = CDCRecord(**kwargs)
        self._log.append(rec)
        return rec
