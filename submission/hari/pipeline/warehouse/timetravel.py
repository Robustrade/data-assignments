"""Point-in-time reads, restore exports and warehouse rebuilds.

Point in time means *source commit time* (``changed_at``). Whole source transactions
share one ``changed_at``, so every table is consistent with the others at any ``ts``.
"""

from __future__ import annotations

import json
from datetime import datetime

from pipeline.cdc.reader import Event
from pipeline.lake.writer import load_events, naive
from pipeline.warehouse import merge
from pipeline.warehouse.model import WarehouseModel

CHUNK = 500


def as_of(conn, model: WarehouseModel, ts: datetime) -> dict[str, list[dict]]:
    """Every table as it was at ``ts``: ``{table: [row dict, ...]}`` ordered by key."""
    stamp = naive(ts)
    result: dict[str, list[dict]] = {}
    for name, tm in model.tables.items():
        hist = model.history_name(name)
        cols = ", ".join(tm.names)
        part = ", ".join(tm.pk)
        rel = conn.execute(
            f"SELECT {cols} FROM ("
            f"  SELECT *, row_number() OVER (PARTITION BY {part} ORDER BY seq DESC) AS rn "
            f"  FROM {hist} WHERE valid_from <= ?"
            f") WHERE rn = 1 AND op <> 'delete' ORDER BY {part}",
            [stamp],
        )
        names = [d[0] for d in rel.description]
        result[name] = [dict(zip(names, row, strict=True)) for row in rel.fetchall()]
    return result


def _key_json(model: WarehouseModel, table: str, key) -> str:
    source_pk = model.tables[table].source_pk
    if isinstance(key, dict):
        parts = key
    else:
        values = key if isinstance(key, (tuple, list)) else (key,)
        parts = dict(zip(source_pk, values, strict=True))
    return json.dumps(parts, sort_keys=True, separators=(",", ":"))


def export_as_of(
    conn, model: WarehouseModel, table: str, keys, ts: datetime
) -> list[dict]:
    """Rows of ``table`` as they were at ``ts``, in *source* form, read from the lake.

    The lake holds the raw values (paise, ISO text, unhashed PII), so the rows can be
    written straight back to the source through ``ShopApp.restore_row``. Keys that did
    not exist (or were deleted) at ``ts`` are omitted.
    """
    out = []
    for key in keys:
        row = conn.execute(
            'SELECT op, "after" FROM lake.cdc_events WHERE table_name = ? AND pk = ? '
            "AND changed_at <= ? ORDER BY seq DESC LIMIT 1",
            [table, _key_json(model, table, key), naive(ts)],
        ).fetchone()
        if row is None or row[0] == "delete":
            continue
        out.append(json.loads(row[1]))
    return out


def _truncate(conn, model: WarehouseModel) -> None:
    for name in model.tables:
        conn.execute(f"DELETE FROM {model.current_name(name)}")
        conn.execute(f"DELETE FROM {model.history_name(name)}")


def _replay(conn, model: WarehouseModel, events: list[Event]) -> None:
    for i in range(0, len(events), CHUNK):
        merge.apply(conn, model, events[i : i + CHUNK])


def rebuild(conn, model: WarehouseModel) -> int:
    """Truncate the warehouse and replay the whole lake. Returns events replayed."""
    events = load_events(conn)
    _truncate(conn, model)
    _replay(conn, model, events)
    return len(events)


def rebuild_to(
    conn, model: WarehouseModel, seq: int | None = None, ts: datetime | None = None
) -> int:
    """Replay the lake only up to ``seq`` (or commit time ``ts``), ending on a transaction
    boundary. Returns the last sequence number replayed (0 if none)."""
    if (seq is None) == (ts is None):
        raise ValueError("pass exactly one of seq / ts")
    events = load_events(conn, upto_seq=seq, upto_ts=ts)
    if events:
        last = events[-1]
        rest = load_events(conn)
        for e in rest:
            if (
                e.seq > last.seq
                and e.txn_id == last.txn_id
                and e.txn_id
                and not e.txn_id.startswith("auto-")
            ):
                events.append(e)
            elif e.seq > last.seq:
                break
    _truncate(conn, model)
    _replay(conn, model, events)
    return events[-1].seq if events else 0
