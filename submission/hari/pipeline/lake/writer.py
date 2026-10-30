"""The lake: every change event, append-only, unique on ``seq``."""

from __future__ import annotations

from datetime import UTC, datetime

from pipeline.cdc.reader import Event, dump_json, load_json


def naive(moment: datetime) -> datetime:
    """UTC wall-clock time without tzinfo (what DuckDB TIMESTAMP columns hold).

    A datetime that already has no tzinfo is taken to be UTC.
    """
    if moment.tzinfo is None:
        return moment
    return moment.astimezone(UTC).replace(tzinfo=None)


def append(
    conn,
    events: list[Event],
    batch_id: str,
    ingested_at: datetime,
    contract_version: int,
) -> int:
    """Append events the lake does not hold yet (dedupe on ``seq``). Returns rows added."""
    if not events:
        return 0
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _stg_lake AS SELECT * FROM lake.cdc_events LIMIT 0"
    )
    stamp = naive(ingested_at)
    seen: set[int] = set()
    rows = []
    for e in events:
        if e.seq in seen:
            continue
        seen.add(e.seq)
        rows.append(
            [
                e.seq,
                e.txn_id,
                e.schema_version,
                e.table_name,
                e.op,
                dump_json(e.pk),
                dump_json(e.before),
                dump_json(e.after),
                naive(e.changed_at),
                batch_id,
                stamp,
                contract_version,
            ]
        )
    conn.executemany("INSERT INTO _stg_lake VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    before = conn.execute("SELECT COUNT(*) FROM lake.cdc_events").fetchone()[0]
    conn.execute(
        "INSERT INTO lake.cdc_events SELECT * FROM _stg_lake s "
        "WHERE s.seq NOT IN (SELECT seq FROM lake.cdc_events)"
    )
    after = conn.execute("SELECT COUNT(*) FROM lake.cdc_events").fetchone()[0]
    return after - before


def load_events(
    conn, upto_seq: int | None = None, upto_ts: datetime | None = None
) -> list[Event]:
    """Read events back from the lake in ``seq`` order (for rebuilds and exports)."""
    where, params = [], []
    if upto_seq is not None:
        where.append("seq <= ?")
        params.append(upto_seq)
    if upto_ts is not None:
        where.append("changed_at <= ?")
        params.append(naive(upto_ts))
    sql = (
        'SELECT seq, txn_id, schema_version, table_name, op, pk, "before", "after", '
        "changed_at FROM lake.cdc_events"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY seq"
    out = []
    for r in conn.execute(sql, params).fetchall():
        out.append(
            Event(
                seq=r[0],
                txn_id=r[1],
                schema_version=r[2],
                table_name=r[3],
                op=r[4],
                pk=load_json(r[5]),
                before=load_json(r[6]),
                after=load_json(r[7]),
                changed_at=r[8].replace(tzinfo=UTC),
            )
        )
    return out
