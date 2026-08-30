"""Ingestion layer — reads changes, gates them, lands them in the lake.

Delivery semantics, stated once so the rest of the system can rely on it:

    Transport is **at-least-once**. The lake write is idempotent under the
    ``(table_name, event_id)`` merge key. The checkpoint is derived from the
    manifest, which is only appended after the data files are durable.
    Therefore a crash between the data write and the manifest append produces a
    *duplicate*, never a *loss*, and the warehouse — being a pure function of
    the deduplicated lake — is unaffected. Net effect: effectively-exactly-once.

Ordering is by ``event_id``, never by arrival time and never by wall clock.
Replaying the same events in a different order produces a byte-identical
warehouse, which is the property the out-of-order test asserts.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import statistics
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from cdclake import contracts as contracts_mod
from cdclake.config import (
    CONTRACTS_DIR,
    TRACKED_TABLES,
    Workspace,
    parse_iso,
    utcnow_iso,
)
from cdclake.gate import SchemaGate, guard_not_halted
from cdclake.lake import BatchRecord, Lake, LakeRecord, append_ndjson, pk_hash

log = logging.getLogger("cdclake.ingest")

DEFAULT_BATCH_SIZE = 500


@dataclass(frozen=True)
class RawChange:
    """One change as the source presents it, before any interpretation."""

    event_id: int
    table_name: str
    op: str
    pk: str
    before: str | None
    after: str | None
    committed_at: str


class CdcSource(Protocol):
    """The seam between this pipeline and however changes are captured.

    ``TriggerAuditSource`` below is the implemented backend. A production
    deployment swaps in a WAL/logical-replication backend behind this same
    interface: ``event_id`` becomes the LSN and ``high_watermark`` becomes the
    slot's confirmed flush position. Nothing downstream changes — see
    docs/adr/0001.
    """

    def read(self, after_event_id: int, limit: int) -> list[RawChange]: ...

    def high_watermark(self) -> int: ...


class TriggerAuditSource:
    """Reads the ``cdc_events`` audit table written by the source triggers."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        tables: Sequence[str] = TRACKED_TABLES,
    ) -> None:
        self.conn = conn
        self.tables = tuple(tables)

    def read(self, after_event_id: int, limit: int) -> list[RawChange]:
        placeholders = ", ".join("?" for _ in self.tables)
        rows = self.conn.execute(
            "SELECT event_id, table_name, op, pk, before_image, after_image, "  # nosec B608
            "committed_at FROM cdc_events "
            f"WHERE event_id > ? AND table_name IN ({placeholders}) "
            "ORDER BY event_id LIMIT ?",
            (after_event_id, *self.tables, limit),
        ).fetchall()
        return [
            RawChange(
                event_id=r["event_id"],
                table_name=r["table_name"],
                op=r["op"],
                pk=r["pk"],
                before=r["before_image"],
                after=r["after_image"],
                committed_at=r["committed_at"],
            )
            for r in rows
        ]

    def high_watermark(self) -> int:
        row = self.conn.execute("SELECT max(event_id) FROM cdc_events").fetchone()
        return int(row[0] or 0)


@dataclass
class BatchResult:
    batch: BatchRecord
    lag_p50_seconds: float
    lag_max_seconds: float
    duration_ms: int


class IngestionRunner:
    """One micro-batch of the CDC pipeline, from gate to durable lake write."""

    def __init__(
        self,
        workspace: Workspace,
        source: CdcSource,
        source_conn: sqlite3.Connection,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        contracts_dir: Path = CONTRACTS_DIR,
        tables: tuple[str, ...] = TRACKED_TABLES,
    ) -> None:
        self.ws = workspace
        self.source = source
        self.source_conn = source_conn
        self.batch_size = batch_size
        self.lake = Lake(workspace)
        self.gate = SchemaGate(workspace, contracts_dir, tables)
        self.contracts_dir = contracts_dir
        self.tables = tables

    # ── one batch ───────────────────────────────────────────────────────────
    def run_batch(
        self, *, simulate_crash_before_manifest: bool = False
    ) -> BatchResult | None:
        """Returns ``None`` when there is nothing new to ingest.

        Raises :class:`~cdclake.gate.IngestionHalted` if the pipeline is halted
        or if the gate finds a breaking change. Nothing is written in either
        case: the gate runs before a single row is read.
        """
        guard_not_halted(self.ws)

        checkpoint = self.lake.checkpoint()
        self.gate.enforce(self.source_conn, checkpoint=checkpoint)

        started_at = utcnow_iso()
        clock = time.perf_counter()

        changes = self.source.read(checkpoint, self.batch_size)
        if not changes:
            return None

        versions = {
            table: contracts_mod.load_contract(
                table, self.contracts_dir
            ).contract_version
            for table in self.tables
        }
        batch_id = self.lake.next_batch_id()
        ingested_at = utcnow_iso()
        records = [
            LakeRecord(
                event_id=c.event_id,
                table_name=c.table_name,
                op=c.op,
                pk=c.pk,
                pk_hash=pk_hash(c.pk),
                before=c.before,
                after=c.after,
                committed_at=c.committed_at,
                ingested_at=ingested_at,
                batch_id=batch_id,
                contract_version=versions[c.table_name],
            )
            for c in changes
        ]

        batch = self.lake.write_batch(
            records,
            batch_id=batch_id,
            checkpoint_before=checkpoint,
            started_at=started_at,
            simulate_crash_before_manifest=simulate_crash_before_manifest,
        )

        lags = [
            (parse_iso(ingested_at) - parse_iso(c.committed_at)).total_seconds()
            for c in changes
        ]
        result = BatchResult(
            batch=batch,
            lag_p50_seconds=round(statistics.median(lags), 3),
            lag_max_seconds=round(max(lags), 3),
            duration_ms=int((time.perf_counter() - clock) * 1000),
        )
        self._record_metrics(result)
        log.info(
            "batch %s: %s events, checkpoint %s -> %s, lag p50 %.3fs max %.3fs",
            batch.batch_id,
            batch.event_count,
            batch.checkpoint_before,
            batch.checkpoint_after,
            result.lag_p50_seconds,
            result.lag_max_seconds,
        )
        return result

    def run_until_caught_up(self, *, max_batches: int = 1000) -> list[BatchResult]:
        results: list[BatchResult] = []
        for _ in range(max_batches):
            outcome = self.run_batch()
            if outcome is None:
                break
            results.append(outcome)
        return results

    def backlog(self) -> int:
        """Events committed at the source but not yet durable in the lake."""
        return max(self.source.high_watermark() - self.lake.checkpoint(), 0)

    # ── observability ───────────────────────────────────────────────────────
    def _record_metrics(self, result: BatchResult) -> None:
        append_ndjson(
            self.ws.metrics_path,
            json.dumps(
                {
                    "emitted_at": utcnow_iso(),
                    "batch_id": result.batch.batch_id,
                    "event_count": result.batch.event_count,
                    "checkpoint_before": result.batch.checkpoint_before,
                    "checkpoint_after": result.batch.checkpoint_after,
                    "lag_p50_seconds": result.lag_p50_seconds,
                    "lag_max_seconds": result.lag_max_seconds,
                    "duration_ms": result.duration_ms,
                    "backlog_after": self.backlog(),
                },
                separators=(",", ":"),
            ),
        )
