"""The pipeline: one atomic batch at a time (``run_once``) and the loops around it."""

from __future__ import annotations

import logging
import signal
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import duckdb

from pipeline import platform
from pipeline.cdc import reader, state
from pipeline.cdc.reader import Event
from pipeline.cdc.state import FAILED, PAUSED, RUNNING, STOPPED
from pipeline.clock import Clock, parse_iso
from pipeline.config import Config
from pipeline.contract import (
    Contract,
    check_event_values,
    diff_contracts,
    diff_live,
    introspect,
    load_contract,
)
from pipeline.errors import (
    PipelineStateError,
    QualityBlocked,
    SchemaContractViolation,
    SimulatedCrash,
)
from pipeline.lake import writer as lake
from pipeline.quality import checks
from pipeline.quality.rules import Ctx
from pipeline.warehouse import merge, timetravel
from pipeline.warehouse.model import build_model, ensure_tables

log = logging.getLogger("cdc")
log.addHandler(logging.NullHandler())  # the application decides where logs go


@dataclass
class BatchResult:
    events: int
    lag_events: int
    freshness_s: float
    status: str


@dataclass
class CatchUpResult:
    batches: int
    events: int
    lag_events: int
    freshness_s: float
    status: str


class _Blocked(Exception):
    def __init__(self, offending):
        self.offending = offending


class Pipeline:
    def __init__(
        self,
        config: Config,
        clock: Clock | None = None,
        owner: str = "pipeline",
        fault_after: int | None = None,
    ):
        self.config = config
        self.clock = clock or Clock()
        self.owner = owner
        self.fault_after = fault_after
        self.conn = platform.connect(config.platform_db)
        self.contracts: dict[int, Contract] = {}
        self.contract: Contract | None = None
        self.model = None
        self.ctx: Ctx | None = None
        self._stop_requested = False
        try:
            self._refresh()
        except (
            duckdb.Error,
            RuntimeError,
        ):  # platform not initialised yet; init() will do it
            log.debug("registry not available before init()")

    # ------------------------------------------------------------------ setup

    def close(self) -> None:
        self.conn.close()

    def init(self) -> None:
        """Create schemas, state and warehouse tables. Safe to run any number of times."""
        now = self.clock.now()
        platform.migrate(self.conn, now)
        state.init_state(self.conn, now)
        if not state.load_contracts(self.conn):
            first = load_contract(self.config.contract_path)
            state.register_contract(self.conn, first, diff_contracts(None, first), now)
        self._refresh()
        ensure_tables(self.conn, self.model)

    def _refresh(self) -> None:
        self.contracts = state.load_contracts(self.conn)
        if not self.contracts:
            raise RuntimeError("no contract registered; call init()")
        self.contract = self.contracts[max(self.contracts)]
        self.model = build_model(list(self.contracts.values()))
        self.ctx = Ctx(self.model, self.contract)

    def _open_source(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.config.source_db), isolation_level=None)
        conn.execute("PRAGMA query_only = ON")
        return conn

    # ------------------------------------------------------------------ status

    def status(self) -> dict:
        return state.get_status(self.conn)

    def checkpoint(self) -> int:
        return state.get_checkpoint(self.conn)

    def measure_lag(self) -> tuple[int, float]:
        """``(lag_events, freshness_s)`` right now, measured against the source (read-only)."""
        src = self._open_source()
        try:
            return reader.lag_and_freshness(
                src, state.get_checkpoint(self.conn), self.clock.now()
            )
        finally:
            src.close()

    def _fail(self, reason: str, details) -> None:
        now = self.clock.now()
        state.set_status(self.conn, FAILED, reason, now)
        state.add_alert(self.conn, now, "ERROR", FAILED, reason, details)
        log.error("pipeline FAILED: %s", reason)

    def _stop(self, violations: list[dict]) -> None:
        now = self.clock.now()
        reason = "schema contract violated: " + "; ".join(
            v["message"] for v in violations[:3]
        )
        state.set_status(self.conn, STOPPED, reason, now)
        lag, fresh = self.measure_lag()
        state.record_metrics(self.conn, now, 0, lag, fresh)
        state.add_alert(
            self.conn, now, "ERROR", STOPPED, reason, {"violations": violations}
        )
        log.error("pipeline STOPPED: %s", reason)
        raise SchemaContractViolation(violations)

    def pause(self, reason: str = "paused by operator") -> None:
        now = self.clock.now()
        state.set_status(self.conn, PAUSED, reason, now)
        state.add_alert(self.conn, now, "INFO", PAUSED, reason)

    def resume(
        self,
        quarantine_seqs=None,
        actor: str = "operator",
        reason: str = "",
    ) -> None:
        """Return to ``RUNNING``.

        From ``STOPPED`` the live schema must match the latest registered contract; from
        ``FAILED`` the listed sequence numbers are loaded into ``quality.quarantine``
        instead of blocking (recorded in ``cdc.alerts``).
        """
        current = self.status()["status"]
        now = self.clock.now()
        if current == RUNNING:
            return
        self._refresh()
        if current == STOPPED:
            src = self._open_source()
            try:
                violations, _ = diff_live(self.contract, introspect(src))
            finally:
                src.close()
            if violations:
                raise SchemaContractViolation(violations)
        if current == FAILED and quarantine_seqs:
            seqs = sorted(set(quarantine_seqs))
            state.add_overrides(self.conn, seqs, actor, reason, now)
            state.add_alert(
                self.conn,
                now,
                "WARN",
                RUNNING,
                f"operator override: quarantine seqs {seqs}",
                {"actor": actor, "reason": reason, "seqs": seqs},
            )
        state.set_status(self.conn, RUNNING, f"resumed from {current}", now)

    # ------------------------------------------------------------------ lock

    @property
    def lock_ttl_s(self) -> float:
        return 3 * self.config.poll_max_s

    def acquire_lock(self) -> None:
        state.acquire_lock(self.conn, self.owner, self.clock.now(), self.lock_ttl_s)

    def release_lock(self) -> None:
        state.release_lock(self.conn, self.owner)

    # ------------------------------------------------------------------ checks

    def _check_live_schema(self, src) -> None:
        violations, warnings = diff_live(self.contract, introspect(src))
        if warnings:
            reason = "schema warning: " + "; ".join(w["message"] for w in warnings[:3])
            seen = self.conn.execute(
                "SELECT 1 FROM cdc.alerts WHERE level = 'WARN' AND reason = ?", [reason]
            ).fetchone()
            if not seen:
                state.add_alert(
                    self.conn,
                    self.clock.now(),
                    "WARN",
                    RUNNING,
                    reason,
                    {"warnings": warnings},
                )
                log.warning(reason)
        if violations:
            self._stop(violations)

    def _check_events(self, events: list[Event]) -> None:
        problems = []
        for e in events:
            contract = self.contracts.get(e.schema_version)
            if contract is None:
                problems.append(
                    {
                        "kind": "unregistered_schema_version",
                        "seq": e.seq,
                        "message": f"event seq {e.seq} has unregistered schema_version {e.schema_version}",
                    }
                )
                continue
            for p in check_event_values(contract, e):
                problems.append({"kind": "event_value", **p})
        if problems:
            self._stop(problems)

    # ------------------------------------------------------------------ one batch

    def _commit_batch(self, events: list[Event], checkpoint_to: int | None) -> dict:
        """Lake + warehouse + quality + checkpoint in one DuckDB transaction."""
        now = self.clock.now()
        conn = self.conn
        batch_id = f"b{events[0].seq}_{events[-1].seq}"
        conn.begin()
        try:
            lake.append(conn, events, batch_id, now, self.contract.version)
            if self.fault_after is not None:
                merge.apply(conn, self.model, events[: self.fault_after])
                raise SimulatedCrash(f"fault injected after {self.fault_after} events")
            merge.apply(conn, self.model, events)
            overrides = state.pending_overrides(conn)
            report = checks.run(
                conn, self.ctx, events, batch_id, now, frozenset(overrides)
            )
            if report["blocking"]:
                raise _Blocked(report["blocking"])
            if checkpoint_to is not None:
                state.set_checkpoint(conn, checkpoint_to, now)
            state.mark_overrides_applied(
                conn, overrides & {e.seq for e in events}, batch_id
            )
            if report["quarantined"]:
                state.add_alert(
                    conn,
                    now,
                    "WARN",
                    RUNNING,
                    f"{len(report['quarantined'])} row(s) quarantined in batch {batch_id}",
                    {"quarantined": report["quarantined"]},
                )
            conn.commit()
            return report
        except _Blocked as blocked:
            conn.rollback()
            seqs = sorted({o["seq"] for o in blocked.offending})
            self._fail(
                f"blocking quality check failed for seq {seqs}",
                {"offending": blocked.offending, "seqs": seqs},
            )
            raise QualityBlocked(blocked.offending) from None
        except Exception as exc:
            conn.rollback()
            self._fail(f"{type(exc).__name__}: {exc}", None)
            raise
        except BaseException:
            conn.rollback()
            raise

    def run_once(self, max_events: int | None = None) -> BatchResult:
        """Process one batch. Does nothing unless the status is ``RUNNING``.

        Raises ``SchemaContractViolation`` (status ``STOPPED``) or ``QualityBlocked``
        (status ``FAILED``) after recording the status and alert; nothing is written for
        the batch in either case.
        """
        current = self.status()
        if current["status"] != RUNNING:
            lag, fresh = self.measure_lag()
            return BatchResult(0, lag, fresh, current["status"])
        self._refresh()
        limit = max_events or self.config.batch_size
        src = self._open_source()
        try:
            self._check_live_schema(src)
            checkpoint = state.get_checkpoint(self.conn)
            events = reader.read_batch(src, checkpoint, limit)
            if not events:
                lag, fresh = reader.lag_and_freshness(src, checkpoint, self.clock.now())
                state.record_metrics(self.conn, self.clock.now(), 0, lag, fresh)
                return BatchResult(0, lag, fresh, RUNNING)
            self._check_events(events)
            waited = max(0.0, (self.clock.now() - events[0].changed_at).total_seconds())
            self._commit_batch(events, events[-1].seq)
            now = self.clock.now()
            lag, fresh = reader.lag_and_freshness(src, events[-1].seq, now)
            state.record_metrics(self.conn, now, len(events), lag, fresh)
        finally:
            src.close()
        self._freshness_alerts(waited)
        return BatchResult(len(events), lag, fresh, RUNNING)

    def _freshness_alerts(self, waited: float) -> None:
        cfg = self.config
        level = None
        if waited > cfg.freshness_crit_s:
            level, limit = "CRITICAL", cfg.freshness_crit_s
        elif waited > cfg.freshness_warn_s:
            level, limit = "WARN", cfg.freshness_warn_s
        if level:
            reason = f"freshness {waited:.0f}s exceeds {limit:.0f}s"
            state.add_alert(
                self.conn,
                self.clock.now(),
                level,
                RUNNING,
                reason,
                {"freshness_s": waited},
            )
            log.warning(reason)

    def process_events(
        self, events: list[Event], advance_checkpoint: bool = False
    ) -> None:
        """Apply hand-made events through the normal batch path (tests, replays)."""
        if self.status()["status"] != RUNNING:
            raise PipelineStateError("pipeline is not RUNNING")
        self._refresh()
        events = sorted(events, key=lambda e: e.seq)
        self._check_events(events)
        target = None
        if advance_checkpoint:
            target = max(state.get_checkpoint(self.conn), events[-1].seq)
        self._commit_batch(events, target)

    # ------------------------------------------------------------------ loops

    def run_until_caught_up(self, max_events: int | None = None) -> CatchUpResult:
        """Drain the backlog in batches. Errors from ``run_once`` propagate."""
        self.acquire_lock()
        batches = total = 0
        result = BatchResult(0, 0, 0.0, self.status()["status"])
        try:
            while True:
                state.heartbeat(self.conn, self.owner, self.clock.now())
                result = self.run_once(max_events)
                if result.events:
                    batches += 1
                    total += result.events
                if (
                    result.status != RUNNING
                    or result.events == 0
                    or result.lag_events == 0
                ):
                    break
        finally:
            self.release_lock()
        return CatchUpResult(
            batches, total, result.lag_events, result.freshness_s, result.status
        )

    def request_stop(self) -> None:
        self._stop_requested = True

    def run_forever(self, stop_after: int | None = None) -> str:
        """Poll with adaptive back-off until stopped. Returns the final status.

        No wait while there is a backlog; when idle the wait grows from ``poll_min_s`` to
        ``poll_max_s``. Exits on ``STOPPED`` / ``FAILED``, on SIGTERM / Ctrl-C, or after
        ``stop_after`` cycles (tests).
        """
        cfg = self.config
        self.acquire_lock()
        self._stop_requested = False
        previous = None
        try:
            previous = signal.signal(signal.SIGTERM, lambda *_: self.request_stop())
        except ValueError:  # not in the main thread
            previous = None
        wait = 0.0
        cycles = 0
        try:
            while not self._stop_requested:
                if stop_after is not None and cycles >= stop_after:
                    break
                cycles += 1
                state.heartbeat(self.conn, self.owner, self.clock.now())
                try:
                    result = self.run_once()
                except (SchemaContractViolation, QualityBlocked):
                    break
                if result.status in (STOPPED, FAILED):
                    break
                if result.lag_events > 0:
                    wait = 0.0
                    continue
                if result.events > 0:
                    wait = cfg.poll_min_s
                else:
                    wait = (
                        cfg.poll_min_s if wait == 0 else min(wait * 2, cfg.poll_max_s)
                    )
                self.clock.sleep(wait)
        except KeyboardInterrupt:
            log.info("interrupted; shutting down")
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
            self.release_lock()
        return self.status()["status"]

    # ------------------------------------------------------------------ bootstrap

    def bootstrap(self, on_snapshot_start: Callable[[], None] | None = None) -> int:
        """Initial load for a source that already holds data (README §4).

        Records ``S0 = max(seq)``, reads one consistent snapshot of every table, loads it
        as ``snapshot`` events (negative sequence numbers, so real events always sort
        after them) and sets the checkpoint to ``S0``. Rows changed during the snapshot
        are re-applied by their later events. Returns ``S0``.
        """
        if self.status()["status"] != RUNNING:
            raise PipelineStateError("pipeline is not RUNNING")
        if (
            state.get_checkpoint(self.conn) != 0
            or self.conn.execute("SELECT COUNT(*) FROM lake.cdc_events").fetchone()[0]
        ):
            raise PipelineStateError("bootstrap needs an empty lake and checkpoint 0")
        self._refresh()
        src = self._open_source()
        try:
            self._check_live_schema(src)
            s0 = src.execute("SELECT COALESCE(MAX(seq), 0) FROM change_log").fetchone()[
                0
            ]
            row = src.execute(
                "SELECT changed_at FROM change_log WHERE seq = ?", (s0,)
            ).fetchone()
            at = parse_iso(row[0]) if row else self.clock.now()
            if on_snapshot_start:
                on_snapshot_start()
            rows: list[tuple[str, dict, dict]] = []
            src.execute("BEGIN")
            try:
                for tname, table in self.contract.tables.items():
                    cols = ", ".join(table.column_names)
                    order = ", ".join(table.primary_key)
                    for values in src.execute(
                        f"SELECT {cols} FROM {tname} ORDER BY {order}"
                    ):
                        image = dict(zip(table.column_names, values, strict=True))
                        rows.append(
                            (tname, {k: image[k] for k in table.primary_key}, image)
                        )
            finally:
                src.execute("COMMIT")
        finally:
            src.close()
        total = len(rows)
        events = [
            Event(
                seq=i - total,
                txn_id="snapshot",
                schema_version=self.contract.version,
                table_name=t,
                op="snapshot",
                pk=pk,
                before=None,
                after=image,
                changed_at=at,
            )
            for i, (t, pk, image) in enumerate(rows)
        ]
        if events:
            self._check_events(events)
            self._commit_batch(events, s0)
        else:
            state.set_checkpoint(self.conn, s0, self.clock.now())
        return s0

    # ------------------------------------------------------------------ schema evolution

    def approve_contract(self, new_contract: Contract) -> None:
        """Register contract version N+1, migrate the warehouse, re-stamp the CDC triggers.

        The source must already match the new contract. History is never rewritten; call
        ``resume()`` afterwards when the pipeline was ``STOPPED``.
        """
        from source import ddl
        from source.db import connect

        self._refresh()
        if new_contract.version != self.contract.version + 1:
            raise ValueError(
                f"next contract version must be {self.contract.version + 1}"
            )
        # The CDC triggers are re-created below, so a missing trigger is not a blocker here.
        src = self._open_source()
        try:
            violations = [
                v
                for v in diff_live(new_contract, introspect(src))[0]
                if v["kind"] != "trigger_missing"
            ]
        finally:
            src.close()
        if violations:
            raise SchemaContractViolation(violations)
        writer = connect(str(self.config.source_db))
        try:
            ddl.regenerate_cdc_triggers(writer, new_contract)
        finally:
            writer.close()
        src = self._open_source()
        try:
            violations = diff_live(new_contract, introspect(src))[0]
        finally:
            src.close()
        if violations:
            raise SchemaContractViolation(violations)
        now = self.clock.now()
        diff = diff_contracts(self.contract, new_contract)
        self.conn.begin()
        try:
            state.register_contract(self.conn, new_contract, diff, now)
            self._refresh()
            ensure_tables(self.conn, self.model)
            state.add_alert(
                self.conn,
                now,
                "INFO",
                self.status()["status"],
                f"contract version {new_contract.version} approved",
                {"diff": diff},
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    # ------------------------------------------------------------------ time travel & repair

    def as_of(self, ts: datetime) -> dict[str, list[dict]]:
        self._refresh()
        return timetravel.as_of(self.conn, self.model, ts)

    def export_as_of(self, table: str, keys, ts: datetime) -> list[dict]:
        self._refresh()
        return timetravel.export_as_of(self.conn, self.model, table, keys, ts)

    def _require_paused(self) -> None:
        if self.status()["status"] != PAUSED:
            raise PipelineStateError(
                "pause() the pipeline before rebuilding the warehouse"
            )
        self._refresh()

    def rebuild(self) -> int:
        """Truncate the warehouse and replay the whole lake (pipeline must be PAUSED)."""
        self._require_paused()
        self.conn.begin()
        try:
            count = timetravel.rebuild(self.conn, self.model)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return count

    def rebuild_to(self, seq: int | None = None, ts: datetime | None = None) -> int:
        """Rebuild only up to ``seq`` / commit time ``ts``; the checkpoint moves back to
        the last replayed event, so ``resume()`` replays forward. Returns that sequence.
        """
        self._require_paused()
        self.conn.begin()
        try:
            last = timetravel.rebuild_to(self.conn, self.model, seq=seq, ts=ts)
            state.set_checkpoint(self.conn, last, self.clock.now())
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return last

    def verify(self) -> dict:
        """Run every parity rule over the whole warehouse (read-only, nothing recorded)."""
        self._refresh()
        self.conn.begin()
        try:
            return checks.run(
                self.conn, self.ctx, None, "verify", self.clock.now(), persist=False
            )
        finally:
            self.conn.rollback()
