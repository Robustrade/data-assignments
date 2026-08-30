"""Warehouse layer — curated snapshot, history, and point-in-time restore.

dbt owns the transformation SQL; this module owns *invoking* it and querying
the result. The warehouse is a pure function of the lake: every model is
rebuilt from the change log, so a rebuild after a replay is a no-op and a
rebuild after a gap is self-healing.

Point-in-time restore has exactly one implementation, ``AS_OF_SQL`` below.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from cdclake.config import DBT_PROJECT_DIR, TRACKED_TABLES, Workspace

log = logging.getLogger("cdclake.warehouse")

#: Reconstruct an entity as it stood at a point in *time*.
#:
#: Semantics: the latest version committed at or before the given instant.
#: Ordering by ``_version`` rather than trusting ``valid_to`` is what makes that
#: well defined when two changes to the same key share a commit timestamp — a
#: real possibility at the millisecond resolution the source clock provides,
#: which produces a zero-length interval that a
#: ``valid_from <= t < valid_to`` predicate would skip entirely.
#:
#: The consequence is worth stating plainly: if two versions of a row committed
#: inside the same millisecond, no timestamp can separate them. Use
#: :data:`AS_OF_VERSION_SQL` when you need to land between two specific
#: versions. This is the same distinction Delta Lake draws between
#: ``TIMESTAMP AS OF`` and ``VERSION AS OF``, and for the same reason.
AS_OF_SQL = """
select *
from dim_{entity}_history
where valid_from <= strptime(?, '%Y-%m-%dT%H:%M:%S.%fZ')
qualify row_number() over (partition by pk_hash order by _version desc) = 1
"""

#: Reconstruct an entity as at a specific change *version*.
#:
#: The exact primitive: event ids are a total order over the change log, so this
#: is never ambiguous. Restores and incident forensics should prefer it;
#: timestamps are for humans.
AS_OF_VERSION_SQL = """
select *
from dim_{entity}_history
where _version <= ?
qualify row_number() over (partition by pk_hash order by _version desc) = 1
"""


class DbtFailed(RuntimeError):
    pass


@dataclass
class DbtOutcome:
    success: bool
    command: tuple[str, ...]
    message: str


class Warehouse:
    """Builds and queries the DuckDB warehouse for one workspace."""

    def __init__(
        self,
        workspace: Workspace,
        *,
        project_dir: Path = DBT_PROJECT_DIR,
        tables: Sequence[str] = TRACKED_TABLES,
    ) -> None:
        self.ws = workspace
        self.project_dir = project_dir
        self.tables = tuple(tables)

    # ── dbt ─────────────────────────────────────────────────────────────────
    def _env(self) -> dict[str, str]:
        return {
            "CDCLAKE_LAKE_GLOB": Path(self.ws.lake_events_dir).as_posix()
            + "/**/*.ndjson",
            "CDCLAKE_WAREHOUSE_DB": self.ws.warehouse_db.as_posix(),
        }

    def _invoke(self, *args: str) -> DbtOutcome:
        from dbt.cli.main import dbtRunner  # imported lazily: dbt is heavy

        previous = {k: os.environ.get(k) for k in self._env()}
        os.environ.update(self._env())
        try:
            result = dbtRunner().invoke(
                [
                    *args,
                    "--project-dir",
                    str(self.project_dir),
                    "--profiles-dir",
                    str(self.project_dir),
                ]
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        message = "" if result.success else str(result.exception or "see dbt output")
        return DbtOutcome(bool(result.success), tuple(args), message)

    def build(self, *, fail_fast: bool = True) -> DbtOutcome:
        """Rebuild every model and run every assertion (``dbt build``)."""
        outcome = self._invoke("build")
        if fail_fast and not outcome.success:
            raise DbtFailed(f"dbt build failed: {outcome.message}")
        return outcome

    def run(self, *, fail_fast: bool = True, quiet: bool = True) -> DbtOutcome:
        """Rebuild models without running the assertions.

        Quiet by default: this is the routine refresh at the end of every
        ingestion cycle, and its per-model log lines drown out anything worth
        reading. ``build`` stays loud, because the assertion results are the
        point of running it.
        """
        outcome = self._invoke("run", *(("--quiet",) if quiet else ()))
        if fail_fast and not outcome.success:
            raise DbtFailed(f"dbt run failed: {outcome.message}")
        return outcome

    def test(self) -> DbtOutcome:
        return self._invoke("test")

    # ── querying ────────────────────────────────────────────────────────────
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Open the warehouse database.

        Deliberately not read-only: dbt keeps a read-write handle to the same
        file inside this process, and DuckDB refuses a second connection whose
        configuration differs from the existing one. Read-only would work in
        isolation and fail the moment a build and a query share a process,
        which is exactly what the tests and the CI scripts do.
        """
        return duckdb.connect(str(self.ws.warehouse_db))

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        with self.connect() as conn:
            cursor = conn.execute(sql, list(params or []))
            columns = [d[0] for d in cursor.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def current(self, entity: str) -> list[dict]:
        sql = f"select * from {entity}_current order by pk_hash"  # nosec B608
        return self.query(sql)

    def history(self, entity: str) -> list[dict]:
        return self.query(
            f"select * from dim_{entity}_history order by pk_hash, _version"  # nosec B608
        )

    def as_of(self, entity: str, timestamp: str) -> list[dict]:
        """State of ``entity`` at ``timestamp``, tombstones excluded.

        The rows returned are what would be written back into the source to undo
        everything committed after ``timestamp``.
        """
        rows = self.query(AS_OF_SQL.format(entity=entity), [timestamp])
        return [r for r in rows if not r["is_deleted"]]

    def as_of_version(self, entity: str, version: int) -> list[dict]:
        """State of ``entity`` as at change ``version``, tombstones excluded.

        Prefer this over :meth:`as_of` for a restore: the version is exact,
        whereas a timestamp cannot separate two changes that committed inside
        the same millisecond.
        """
        rows = self.query(AS_OF_VERSION_SQL.format(entity=entity), [version])
        return [r for r in rows if not r["is_deleted"]]

    def row_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table in self.tables:
            sql = f"select count(*) as n from {table}_current"  # nosec B608
            counts[table] = self.query(sql)[0]["n"]
        return counts
