"""Read change events from the source ``change_log``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from pipeline.clock import parse_iso


@dataclass(frozen=True)
class Event:
    seq: int
    txn_id: str | None
    schema_version: int
    table_name: str
    op: str  # insert | update | delete | snapshot
    pk: dict
    before: dict | None
    after: dict | None
    changed_at: datetime

    @property
    def image(self) -> dict:
        """The row image this event leaves behind (``before`` for a delete)."""
        return (self.before if self.op == "delete" else self.after) or {}


def dump_json(value: dict | None) -> str | None:
    """Canonical JSON text used in the lake (sorted keys, no spaces)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def load_json(text: str | None) -> dict | None:
    return None if text is None else json.loads(text)


def row_to_event(row) -> Event:
    return Event(
        seq=int(row[0]),
        txn_id=row[1],
        schema_version=int(row[2]),
        table_name=row[3],
        op=row[4],
        pk=json.loads(row[5]),
        before=load_json(row[6]),
        after=load_json(row[7]),
        changed_at=parse_iso(row[8]),
    )


_COLUMNS = (
    'seq, txn_id, schema_version, table_name, op, pk, "before", "after", changed_at'
)


def read_batch(src, after_seq: int, limit: int) -> list[Event]:
    """Events with ``seq > after_seq`` in order, extended to finish the last transaction.

    The source serialises writers, so the events of one transaction are contiguous in
    ``seq``; the batch is extended while the following events share the last ``txn_id``.
    """
    rows = src.execute(
        f"SELECT {_COLUMNS} FROM change_log WHERE seq > ? ORDER BY seq LIMIT ?",
        (after_seq, limit),
    ).fetchall()
    events = [row_to_event(r) for r in rows]
    if not events:
        return events
    last = events[-1]
    cursor = last.seq
    while True:
        more = src.execute(
            f"SELECT {_COLUMNS} FROM change_log WHERE seq > ? ORDER BY seq LIMIT 100",
            (cursor,),
        ).fetchall()
        extra = []
        for r in more:
            if r[1] != last.txn_id:
                break
            extra.append(row_to_event(r))
        events.extend(extra)
        if len(extra) < len(more) or not more:
            break
        cursor = events[-1].seq
    return events


def lag_and_freshness(src, checkpoint: int, now: datetime) -> tuple[int, float]:
    """``lag_events`` = max(seq) - checkpoint; freshness = age of the oldest unprocessed event."""
    top = src.execute("SELECT COALESCE(MAX(seq), 0) FROM change_log").fetchone()[0]
    lag = max(0, int(top) - checkpoint)
    if lag == 0:
        return 0, 0.0
    oldest = src.execute(
        "SELECT changed_at FROM change_log WHERE seq > ? ORDER BY seq LIMIT 1",
        (checkpoint,),
    ).fetchone()[0]
    return lag, max(0.0, (now - parse_iso(oldest)).total_seconds())
