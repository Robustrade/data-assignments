"""Pipeline state kept in DuckDB: checkpoint, status, alerts, registry, lock, overrides."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from pipeline.contract import Contract
from pipeline.errors import PipelineLocked
from pipeline.lake.writer import naive

RUNNING = "RUNNING"
PAUSED = "PAUSED"
STOPPED = "STOPPED"
FAILED = "FAILED"


def init_state(conn, now: datetime) -> None:
    stamp = naive(now)
    if not conn.execute("SELECT COUNT(*) FROM cdc.checkpoint").fetchone()[0]:
        conn.execute("INSERT INTO cdc.checkpoint VALUES (1, 0, ?)", [stamp])
    if not conn.execute("SELECT COUNT(*) FROM cdc.pipeline_status").fetchone()[0]:
        conn.execute(
            "INSERT INTO cdc.pipeline_status VALUES (1, ?, 'initialised', ?, NULL, 0, 0, 0)",
            [RUNNING, stamp],
        )


# ------------------------------------------------------------------ checkpoint


def get_checkpoint(conn) -> int:
    return conn.execute("SELECT last_seq FROM cdc.checkpoint WHERE id = 1").fetchone()[
        0
    ]


def set_checkpoint(conn, seq: int, now: datetime) -> None:
    conn.execute(
        "UPDATE cdc.checkpoint SET last_seq = ?, updated_at = ? WHERE id = 1",
        [seq, naive(now)],
    )


# ------------------------------------------------------------------ status & alerts


def get_status(conn) -> dict:
    row = conn.execute(
        "SELECT status, reason, since, last_batch_at, last_batch_events, lag_events, "
        "freshness_s FROM cdc.pipeline_status WHERE id = 1"
    ).fetchone()
    keys = (
        "status",
        "reason",
        "since",
        "last_batch_at",
        "last_batch_events",
        "lag_events",
        "freshness_s",
    )
    return dict(zip(keys, row, strict=True))


def set_status(conn, status: str, reason: str, now: datetime) -> None:
    conn.execute(
        "UPDATE cdc.pipeline_status SET status = ?, reason = ?, since = ? WHERE id = 1",
        [status, reason, naive(now)],
    )


def record_metrics(
    conn, now: datetime, events: int, lag: int, freshness: float
) -> None:
    conn.execute(
        "UPDATE cdc.pipeline_status SET last_batch_at = ?, last_batch_events = ?, "
        "lag_events = ?, freshness_s = ? WHERE id = 1",
        [naive(now), events, lag, freshness],
    )


def add_alert(
    conn, now: datetime, level: str, status: str | None, reason: str, details=None
) -> None:
    conn.execute(
        "INSERT INTO cdc.alerts (ts, level, status, reason, details) VALUES (?, ?, ?, ?, ?)",
        [
            naive(now),
            level,
            status,
            reason,
            json.dumps(details, default=str) if details is not None else None,
        ],
    )


# ------------------------------------------------------------------ schema registry


def register_contract(
    conn, contract: Contract, diff: list[dict], now: datetime
) -> None:
    conn.execute(
        "INSERT INTO cdc.schema_registry VALUES (?, ?, ?, ?)",
        [
            contract.version,
            json.dumps(contract.to_dict()),
            json.dumps(diff),
            naive(now),
        ],
    )


def load_contracts(conn) -> dict[int, Contract]:
    rows = conn.execute(
        "SELECT version, contract_json FROM cdc.schema_registry ORDER BY version"
    ).fetchall()
    return {v: Contract.from_dict(json.loads(j)) for v, j in rows}


# ------------------------------------------------------------------ single-instance lease


def acquire_lock(conn, owner: str, now: datetime, stale_after_s: float) -> None:
    """Take the lease, or raise ``PipelineLocked`` if another owner's lease is alive."""
    row = conn.execute(
        "SELECT owner, heartbeat_at FROM cdc.pipeline_lock WHERE id = 1"
    ).fetchone()
    if row is not None:
        holder, beat = row
        alive = naive(now) - beat <= timedelta(seconds=stale_after_s)
        if holder != owner and alive:
            raise PipelineLocked(f"pipeline is locked by {holder!r} (heartbeat {beat})")
        conn.execute("DELETE FROM cdc.pipeline_lock WHERE id = 1")
    conn.execute("INSERT INTO cdc.pipeline_lock VALUES (1, ?, ?)", [owner, naive(now)])


def heartbeat(conn, owner: str, now: datetime) -> None:
    conn.execute(
        "UPDATE cdc.pipeline_lock SET heartbeat_at = ? WHERE id = 1 AND owner = ?",
        [naive(now), owner],
    )


def release_lock(conn, owner: str) -> None:
    conn.execute("DELETE FROM cdc.pipeline_lock WHERE id = 1 AND owner = ?", [owner])


# ------------------------------------------------------------------ quarantine overrides


def add_overrides(conn, seqs, actor: str, reason: str, now: datetime) -> None:
    for seq in seqs:
        conn.execute(
            "INSERT INTO cdc.quarantine_overrides VALUES (?, ?, ?, ?, NULL)",
            [seq, actor, reason, naive(now)],
        )


def pending_overrides(conn) -> set[int]:
    rows = conn.execute(
        "SELECT seq FROM cdc.quarantine_overrides WHERE applied_batch IS NULL"
    )
    return {r[0] for r in rows.fetchall()}


def mark_overrides_applied(conn, seqs, batch_id: str) -> None:
    for seq in seqs:
        conn.execute(
            "UPDATE cdc.quarantine_overrides SET applied_batch = ? "
            "WHERE seq = ? AND applied_batch IS NULL",
            [batch_id, seq],
        )
