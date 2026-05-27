"""Append-only Parquet lake writer for CDC events."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config
from pipeline.cdc_event import CDCEvent

LAKE_SCHEMA = pa.schema(
    [
        ("event_id", pa.int64()),
        ("source_table", pa.string()),
        ("op", pa.string()),
        ("pk_json", pa.string()),
        ("row_json", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def _partition_path(source_table: str, event_ts: datetime, run_id: str) -> Path:
    dt = (
        event_ts.astimezone(timezone.utc)
        if event_ts.tzinfo
        else event_ts.replace(tzinfo=timezone.utc)
    )
    return (
        config.LAKE_DIR
        / f"source_table={source_table}"
        / f"dt={dt.date().isoformat()}"
        / f"part-{run_id}.parquet"
    )


def write_events_to_lake(
    events: list[CDCEvent], run_id: str | None = None
) -> list[Path]:
    if not events:
        return []

    run_id = run_id or uuid.uuid4().hex[:12]
    ingested_at = datetime.now(timezone.utc)
    by_partition: dict[tuple[str, str], list[CDCEvent]] = {}

    for event in events:
        dt_key = (
            event.event_ts.astimezone(timezone.utc).date().isoformat()
            if event.event_ts.tzinfo
            else event.event_ts.date().isoformat()
        )
        key = (event.source_table, dt_key)
        by_partition.setdefault(key, []).append(event)

    written: list[Path] = []
    for (source_table, _dt_key), partition_events in by_partition.items():
        path = _partition_path(source_table, partition_events[0].event_ts, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pydict(
            {
                "event_id": [e.event_id for e in partition_events],
                "source_table": [e.source_table for e in partition_events],
                "op": [e.op for e in partition_events],
                "pk_json": [e.pk_json for e in partition_events],
                "row_json": [e.row_json for e in partition_events],
                "event_ts": [e.event_ts for e in partition_events],
                "ingested_at": [ingested_at] * len(partition_events),
            },
            schema=LAKE_SCHEMA,
        )
        pq.write_table(table, path)
        written.append(path)

    return written


def read_all_lake_events(lake_dir: Path | None = None) -> list[CDCEvent]:
    base = lake_dir or config.LAKE_DIR
    if not base.exists():
        return []

    events: list[CDCEvent] = []
    for parquet_file in sorted(base.glob("source_table=*/dt=*/*.parquet")):
        table = pq.ParquetFile(parquet_file).read()
        for row in table.to_pylist():
            events.append(
                CDCEvent(
                    event_id=int(row["event_id"]),
                    source_table=str(row["source_table"]),
                    op=str(row["op"]),
                    pk_json=str(row["pk_json"]),
                    row_json=str(row["row_json"]),
                    event_ts=row["event_ts"],
                )
            )
    events.sort(key=lambda e: e.event_id)
    return events
