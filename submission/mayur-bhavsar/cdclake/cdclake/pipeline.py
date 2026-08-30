"""Composition root — wires the six layers into one runnable platform.

Nothing here contains business logic. It exists so that the CLI, the demo, the
CI scripts and the tests all assemble the platform the same way, rather than
each growing its own slightly different wiring.
"""

from __future__ import annotations

import logging
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from cdclake import source as source_mod
from cdclake.config import CONTRACTS_DIR, TRACKED_TABLES, Workspace
from cdclake.ingest import IngestionRunner, TriggerAuditSource
from cdclake.lake import Lake
from cdclake.warehouse import Warehouse

log = logging.getLogger("cdclake.pipeline")


@dataclass
class Platform:
    workspace: Workspace
    conn: sqlite3.Connection
    lake: Lake
    runner: IngestionRunner
    warehouse: Warehouse

    def ingest(self) -> int:
        """Drain the source into the lake. Returns events ingested."""
        return sum(r.batch.event_count for r in self.runner.run_until_caught_up())

    def refresh_warehouse(self) -> None:
        self.warehouse.run()

    def sync(self) -> int:
        """One full cycle: capture -> lake -> warehouse."""
        ingested = self.ingest()
        self.refresh_warehouse()
        return ingested

    def close(self) -> None:
        self.conn.close()


def build_platform(
    root: Path | str,
    *,
    fresh: bool = False,
    seed: bool = False,
    contracts_dir: Path = CONTRACTS_DIR,
    batch_size: int = 500,
) -> Platform:
    """Assemble a platform rooted at ``root``.

    ``fresh=True`` deletes the workspace first, which is what makes the demo
    and the tests reproducible from a clean state.
    """
    root = Path(root)
    if fresh and root.exists():
        shutil.rmtree(root)

    workspace = Workspace.create(root)
    conn = source_mod.connect(workspace.source_db)
    source_mod.apply_ddl(conn)
    if seed:
        source_mod.seed(conn)

    cdc_source = TriggerAuditSource(conn, TRACKED_TABLES)
    runner = IngestionRunner(
        workspace,
        cdc_source,
        conn,
        batch_size=batch_size,
        contracts_dir=contracts_dir,
    )
    return Platform(
        workspace=workspace,
        conn=conn,
        lake=Lake(workspace),
        runner=runner,
        warehouse=Warehouse(workspace),
    )


def run_reference_pipeline(root: Path | str, *, build: bool = True) -> Platform:
    """Seed, apply the whole workload, ingest and build.

    The single definition of "a full run", shared by the demo, the CI scripts
    and the tests, so none of them can validate a different pipeline from the
    one that ships.
    """
    platform = build_platform(root, fresh=True, seed=True)
    platform.ingest()
    source_mod.run_workload(platform.conn)
    platform.ingest()
    if build:
        platform.warehouse.run()
    return platform


@contextmanager
def temporary_workspace(prefix: str = "cdclake-") -> Iterator[Path]:
    """Throwaway workspace root.

    Cleanup ignores errors: DuckDB caches database instances per process and
    SQLite in WAL mode keeps sidecar files, so on Windows a handle can outlive
    the connection object. Leaving a temp directory behind is not a failure
    worth aborting a validation run for.
    """
    path = Path(tempfile.mkdtemp(prefix=prefix))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def reconcile(platform: Platform) -> dict[str, dict[str, int]]:
    """Row-count reconciliation between the source and the warehouse.

    A cheap, high-signal completeness check: if the counts diverge, either a
    change was lost or a tombstone was mishandled.
    """
    source_counts = source_mod.row_counts(platform.conn)
    warehouse_counts = platform.warehouse.row_counts()
    return {
        table: {
            "source": source_counts[table],
            "warehouse": warehouse_counts[table],
            "delta": warehouse_counts[table] - source_counts[table],
        }
        for table in TRACKED_TABLES
    }
