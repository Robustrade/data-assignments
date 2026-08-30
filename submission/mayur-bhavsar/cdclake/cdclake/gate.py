"""Schema gate and the pipeline state machine — the stop-the-line mechanism.

An incompatible source change is not an exception that scrolls past in a log.
It is a *persisted state transition*:

    RUNNING --breaking delta--> HALTED(reason, checkpoint, diff, detected_at)
    HALTED  --operator acknowledge--> RUNNING

While HALTED the runner refuses to start at all, so a scheduled job cannot
quietly resume ingesting under assumptions that are known to be false. Getting
out requires a human decision, recorded in the state file: either the source
change is reverted, or the contract is republished with an explicit note.

The gate runs *before* the batch reads any rows, so nothing incompatible ever
reaches the lake.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from cdclake import contracts as contracts_mod
from cdclake.config import CONTRACTS_DIR, TRACKED_TABLES, Workspace, utcnow_iso
from cdclake.lake import append_ndjson

log = logging.getLogger("cdclake.gate")

RUNNING = "RUNNING"
HALTED = "HALTED"


class IngestionHalted(RuntimeError):
    """Raised when the pipeline must stop. Carries the diff that caused it."""

    def __init__(self, reason: str, deltas: list[contracts_mod.SchemaDelta]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.deltas = deltas


@dataclass
class PipelineState:
    state: str = RUNNING
    reason: str | None = None
    detected_at: str | None = None
    offending_checkpoint: int | None = None
    contract_diff: list[dict[str, Any]] = field(default_factory=list)
    acknowledged_at: str | None = None
    acknowledged_by: str | None = None
    acknowledgement_note: str | None = None

    @property
    def is_halted(self) -> bool:
        return self.state == HALTED

    @classmethod
    def load(cls, workspace: Workspace) -> PipelineState:
        path = workspace.state_path
        if not path.exists():
            return cls()
        return cls(**json.loads(path.read_text(encoding="utf-8")))

    def save(self, workspace: Workspace) -> None:
        workspace.state_dir.mkdir(parents=True, exist_ok=True)
        workspace.state_path.write_text(
            json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8"
        )


def emit_alert(
    workspace: Workspace, severity: str, event: str, detail: dict[str, Any]
) -> None:
    """Machine-readable alert stream.

    In production this is the webhook to PagerDuty / Slack. Keeping it as a
    file here means the tests can assert that a warning was actually surfaced,
    rather than assuming a log line was emitted.
    """
    append_ndjson(
        workspace.alerts_path,
        json.dumps(
            {
                "emitted_at": utcnow_iso(),
                "severity": severity,
                "event": event,
                **detail,
            },
            separators=(",", ":"),
        ),
    )


class SchemaGate:
    """Compares live source schema against the published contracts."""

    def __init__(
        self,
        workspace: Workspace,
        contracts_dir: Path = CONTRACTS_DIR,
        tables: tuple[str, ...] = TRACKED_TABLES,
    ) -> None:
        self.ws = workspace
        self.contracts_dir = contracts_dir
        self.tables = tables

    def inspect(self, conn: sqlite3.Connection) -> contracts_mod.GateVerdict:
        """Classify drift without changing any state. Pure function."""
        expected = contracts_mod.load_all(self.tables, self.contracts_dir)
        actual: dict[str, contracts_mod.TableContract] = {}
        for table in self.tables:
            try:
                actual[table] = contracts_mod.introspect_table(conn, table)
            except ValueError:
                continue  # absent table is reported as TABLE_DROPPED by diff_all
        return contracts_mod.diff_all(expected, actual)

    def enforce(
        self, conn: sqlite3.Connection, *, checkpoint: int
    ) -> contracts_mod.GateVerdict:
        """Run the gate and act on the verdict.

        Compatible drift is surfaced as a WARN alert and ingestion continues.
        Breaking drift halts the pipeline, persists the reason and re-raises.
        """
        verdict = self.inspect(conn)

        for delta in verdict.warnings:
            log.warning("compatible schema change: %s", delta)
            emit_alert(
                self.ws,
                "WARN",
                "schema_change_compatible",
                {"delta": asdict(delta), "checkpoint": checkpoint},
            )

        if verdict.is_compatible:
            return verdict

        reason = (
            f"{len(verdict.breaking)} incompatible source schema change(s) detected"
        )
        state = PipelineState(
            state=HALTED,
            reason=reason,
            detected_at=utcnow_iso(),
            offending_checkpoint=checkpoint,
            contract_diff=[asdict(d) for d in verdict.breaking],
        )
        state.save(self.ws)

        log.error("INGESTION HALTED — %s\n%s", reason, verdict.render())
        emit_alert(
            self.ws,
            "ERROR",
            "ingestion_halted",
            {
                "reason": reason,
                "checkpoint": checkpoint,
                "deltas": [asdict(d) for d in verdict.breaking],
            },
        )
        raise IngestionHalted(reason, verdict.breaking)


def guard_not_halted(workspace: Workspace) -> None:
    """Refuse to start while HALTED. Called before anything else in a run."""
    state = PipelineState.load(workspace)
    if state.is_halted:
        raise IngestionHalted(
            "pipeline is HALTED and has not been acknowledged: "
            f"{state.reason} (detected at {state.detected_at}). "
            "Inspect _state/pipeline_state.json, then run "
            "`cdclake acknowledge --operator <you> --note <why>`.",
            [contracts_mod.SchemaDelta(**d) for d in state.contract_diff],
        )


def acknowledge(
    workspace: Workspace, *, operator: str, note: str, clear_diff: bool = True
) -> PipelineState:
    """Break glass: an operator takes responsibility and clears the halt."""
    state = PipelineState.load(workspace)
    if not state.is_halted:
        return state
    resumed = PipelineState(
        state=RUNNING,
        reason=None,
        detected_at=None,
        offending_checkpoint=None,
        contract_diff=[] if clear_diff else state.contract_diff,
        acknowledged_at=utcnow_iso(),
        acknowledged_by=operator,
        acknowledgement_note=note,
    )
    resumed.save(workspace)
    emit_alert(
        workspace,
        "INFO",
        "halt_acknowledged",
        {"operator": operator, "note": note, "previous_reason": state.reason},
    )
    log.warning("halt acknowledged by %s: %s", operator, note)
    return resumed


def republish_contracts(
    conn: sqlite3.Connection,
    *,
    contracts_dir: Path = CONTRACTS_DIR,
    tables: tuple[str, ...] = TRACKED_TABLES,
) -> list[Path]:
    """Adopt the live schema as the new contract, bumping the version.

    This is the deliberate half of the break-glass procedure. It is never
    called automatically — accepting a breaking change is a decision about the
    meaning of the data, not something a scheduler should make at 03:00.
    """
    written: list[Path] = []
    for table in tables:
        try:
            live = contracts_mod.introspect_table(conn, table)
        except ValueError:
            continue
        try:
            previous_version = contracts_mod.load_contract(
                table, contracts_dir
            ).contract_version
        except FileNotFoundError:
            previous_version = 0
        published = contracts_mod.TableContract(
            table=live.table,
            contract_version=previous_version + 1,
            primary_key=live.primary_key,
            columns=live.columns,
            foreign_keys=live.foreign_keys,
            unique_constraints=live.unique_constraints,
            indexes=live.indexes,
            description=contracts_mod.TABLE_DESCRIPTIONS.get(table, ""),
        )
        written.append(contracts_mod.write_contract(published, contracts_dir))
    return written
