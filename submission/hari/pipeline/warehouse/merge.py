"""Merge change events into the warehouse (current state + SCD2 history).

The merge is idempotent and order-tolerant:

* history gets one row per event (skipping any ``seq`` already present); ``valid_to`` and
  ``is_current`` of the affected keys are then recomputed in ``seq`` order, so a late
  event lands in its correct position;
* ``_current`` only moves forward: an event is applied if ``seq > _last_seq`` (or the row
  is absent), so older events never overwrite newer state. Deletes leave a tombstone.
"""

from __future__ import annotations

from pipeline.cdc.reader import Event
from pipeline.warehouse.model import (
    CURRENT_META,
    HISTORY_META,
    WarehouseModel,
    convert,
    to_naive,
)


def _business_values(tm, event: Event) -> list:
    image = event.image
    active = {c.name for c in tm.active_columns(event.schema_version)}
    values = []
    for col in tm.columns:
        values.append(
            convert(image.get(col.source), col.kind) if col.name in active else None
        )
    return values


def apply(conn, model: WarehouseModel, events: list[Event]) -> None:
    by_seq = {e.seq: e for e in events}
    ordered = [by_seq[s] for s in sorted(by_seq)]
    by_table: dict[str, list[Event]] = {}
    for event in ordered:
        if event.table_name not in model.tables:
            raise KeyError(f"no warehouse table for {event.table_name}")
        by_table.setdefault(event.table_name, []).append(event)
    for table, table_events in by_table.items():
        _merge_history(conn, model, table, table_events)
        _merge_current(conn, model, table, table_events)


def _merge_history(
    conn, model: WarehouseModel, table: str, events: list[Event]
) -> None:
    tm = model.tables[table]
    hist = model.history_name(table)
    names = tm.names + [n for n, _ in HISTORY_META]
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE _stg_hist AS SELECT * FROM {hist} LIMIT 0"
    )
    rows = []
    for e in events:
        rows.append(
            [
                *_business_values(tm, e),
                to_naive(e.changed_at),
                None,
                False,
                e.op,
                e.seq,
                e.txn_id,
                e.schema_version,
            ]
        )
    marks = ",".join("?" for _ in names)
    conn.executemany(
        f"INSERT INTO _stg_hist ({', '.join(names)}) VALUES ({marks})", rows
    )
    columns = ", ".join(names)
    conn.execute(
        f"INSERT INTO {hist} ({columns}) SELECT {columns} FROM _stg_hist s "
        f"WHERE s.seq NOT IN (SELECT seq FROM {hist})"
    )
    pk = tm.pk
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE _aff AS SELECT DISTINCT {', '.join(pk)} FROM _stg_hist"
    )
    join = " AND ".join(f"h2.{c} = a.{c}" for c in pk)
    part = ", ".join(f"h2.{c}" for c in pk)
    conn.execute(f"""
        UPDATE {hist} h SET valid_to = x.vt, is_current = x.cur
        FROM (
            SELECT h2.seq AS seq,
                   CASE WHEN h2.op = 'delete' THEN h2.valid_from
                        ELSE LEAD(h2.valid_from) OVER w END AS vt,
                   (h2.op <> 'delete' AND LEAD(h2.seq) OVER w IS NULL) AS cur
            FROM {hist} h2 JOIN _aff a ON {join}
            WINDOW w AS (PARTITION BY {part} ORDER BY h2.seq)
        ) x
        WHERE h.seq = x.seq
        """)


def _merge_current(
    conn, model: WarehouseModel, table: str, events: list[Event]
) -> None:
    tm = model.tables[table]
    cur = model.current_name(table)
    names = tm.names + [n for n, _ in CURRENT_META]
    final: dict[tuple, Event] = {}
    for e in events:  # ascending seq: the last event per key wins
        final[tuple(e.pk[c] for c in tm.source_pk)] = e
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE _stg_cur AS SELECT * FROM {cur} LIMIT 0"
    )
    rows = [
        [
            *_business_values(tm, e),
            e.seq,
            e.txn_id,
            e.schema_version,
            e.op == "delete",
            to_naive(e.changed_at),
        ]
        for e in final.values()
    ]
    marks = ",".join("?" for _ in names)
    conn.executemany(
        f"INSERT INTO _stg_cur ({', '.join(names)}) VALUES ({marks})", rows
    )
    match = " AND ".join(f"c.{c} = s.{c}" for c in tm.pk)
    assign = ", ".join(f"{n} = s.{n}" for n in names)
    conn.execute(
        f"UPDATE {cur} c SET {assign} FROM _stg_cur s WHERE {match} AND s._last_seq > c._last_seq"
    )
    columns = ", ".join(names)
    conn.execute(
        f"INSERT INTO {cur} ({columns}) SELECT {columns} FROM _stg_cur s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {cur} c WHERE {match})"
    )
