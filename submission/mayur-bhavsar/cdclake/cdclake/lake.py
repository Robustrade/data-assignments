"""Lake layer — durable, append-only change history.

Design decisions worth defending:

*Files are immutable.* A batch writes new files and never rewrites an existing
one. Nothing in this module can destroy history.

*The payload columns are JSON strings, not nested structs.* ``pk``, ``before``
and ``after`` are stored as text. That keeps the physical lake schema identical
for every table and — more importantly — **the lake schema does not change when
the source schema changes**. A new source column produces different JSON, not a
different file layout, so a schema drift can never corrupt the change log
itself. Typing happens once, downstream, against the contract.

*The checkpoint is derived from the manifest, not stored beside it.* There is no
second source of truth that can drift out of sync with the data.

*Crash safety.* Data files are written to a temporary name and atomically
renamed, then the manifest line is appended and fsynced. A crash between those
two steps leaves orphan files that no reader will ever see (readers resolve
files through the manifest) and re-emits the same events under a new batch id on
restart. Duplicates are absorbed downstream by the ``(table_name, event_id)``
merge key, so the failure mode is a duplicate, never a loss.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from cdclake.config import Workspace, utcnow_iso

#: Physical column order of a lake record. Stable and independent of the source.
LAKE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "table_name",
    "op",
    "pk",
    "pk_hash",
    "before",
    "after",
    "committed_at",
    "ingested_at",
    "batch_id",
    "contract_version",
)


def pk_hash(pk_json: str) -> str:
    """Stable hash of the canonical primary-key JSON.

    Used as the partition/merge key downstream so that composite keys on weak
    entities behave exactly like surrogate keys on strong ones.
    """
    return hashlib.sha256(pk_json.encode("utf-8")).hexdigest()[:16]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class LakeRecord:
    event_id: int
    table_name: str
    op: str
    pk: str
    pk_hash: str
    before: str | None
    after: str | None
    committed_at: str
    ingested_at: str
    batch_id: int
    contract_version: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


@dataclass
class FileEntry:
    path: str
    table_name: str
    rows: int


@dataclass
class BatchRecord:
    batch_id: int
    checkpoint_before: int
    checkpoint_after: int
    event_count: int
    started_at: str
    committed_at: str
    files: list[FileEntry] = field(default_factory=list)

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload, separators=(",", ":"))


def append_ndjson(path: Path, payload: str) -> None:
    """Append one line and fsync it. The fsync is the point: without it the
    manifest can lie about what is durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(payload + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_ndjson(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


class Lake:
    """Append-only change store rooted at ``workspace.lake_dir``."""

    def __init__(self, workspace: Workspace) -> None:
        self.ws = workspace
        self.ws.ensure()

    # ── manifest / checkpoint ───────────────────────────────────────────────
    def batches(self) -> list[dict[str, Any]]:
        return list(read_ndjson(self.ws.manifest_path))

    def checkpoint(self) -> int:
        """Highest source ``event_id`` known to be durably in the lake.

        Derived from the manifest so it can never disagree with the data.
        """
        return max((int(b["checkpoint_after"]) for b in self.batches()), default=0)

    def next_batch_id(self) -> int:
        return max((int(b["batch_id"]) for b in self.batches()), default=0) + 1

    def manifest_files(self) -> list[Path]:
        out: list[Path] = []
        for batch in self.batches():
            for entry in batch["files"]:
                out.append(self.ws.lake_dir / entry["path"])
        return out

    # ── writing ─────────────────────────────────────────────────────────────
    def write_batch(
        self,
        records: Iterable[LakeRecord],
        *,
        batch_id: int,
        checkpoint_before: int,
        started_at: str,
        simulate_crash_before_manifest: bool = False,
    ) -> BatchRecord:
        """Persist one batch. Returns the manifest entry that was committed.

        ``simulate_crash_before_manifest`` exists so the crash-recovery test can
        exercise the real code path rather than a mock of it.
        """
        by_table: dict[str, list[LakeRecord]] = {}
        highest = checkpoint_before
        count = 0
        for record in records:
            by_table.setdefault(record.table_name, []).append(record)
            highest = max(highest, record.event_id)
            count += 1

        entries: list[FileEntry] = []
        ingest_date = utcnow_iso()[:10]
        for table_name, rows in sorted(by_table.items()):
            rel = Path(
                "cdc_events",
                f"table_name={table_name}",
                f"ingest_date={ingest_date}",
                f"batch_{batch_id:06d}.ndjson",
            )
            target = self.ws.lake_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_suffix(".ndjson.tmp")
            with tmp.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(row.to_json() + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)  # atomic publish of an immutable file
            entries.append(
                FileEntry(path=rel.as_posix(), table_name=table_name, rows=len(rows))
            )

        batch = BatchRecord(
            batch_id=batch_id,
            checkpoint_before=checkpoint_before,
            checkpoint_after=highest,
            event_count=count,
            started_at=started_at,
            committed_at=utcnow_iso(),
            files=entries,
        )

        if simulate_crash_before_manifest:
            raise RuntimeError("simulated crash after data write, before manifest")

        append_ndjson(self.ws.manifest_path, batch.to_json())
        return batch

    # ── reading ─────────────────────────────────────────────────────────────
    def records(self) -> Iterator[dict[str, Any]]:
        """Every change record the manifest vouches for, in batch order."""
        for path in self.manifest_files():
            yield from read_ndjson(path)

    def all_files_on_disk(self) -> list[Path]:
        return sorted(self.ws.lake_events_dir.rglob("*.ndjson"))

    def orphan_files(self) -> list[Path]:
        """Files on disk that no manifest entry references — the residue of a
        crash between the data write and the manifest append."""
        known = {p.resolve() for p in self.manifest_files()}
        return [p for p in self.all_files_on_disk() if p.resolve() not in known]

    def gc_orphans(self) -> list[Path]:
        """Remove unreferenced files. Safe by construction: an orphan's events
        are, by definition, above the checkpoint and will be re-read."""
        removed = self.orphan_files()
        for path in removed:
            path.unlink()
        return removed

    # ── addressing for query engines ────────────────────────────────────────
    def glob_pattern(self) -> str:
        """Glob a query engine can hand to ``read_json``.

        Note this reaches every file on disk, including orphans. That is
        deliberate and safe: the warehouse deduplicates on
        ``(table_name, event_id)``, so an orphan contributes nothing new. The
        manifest path is used where exactness matters (replay, audit).
        """
        return (self.ws.lake_events_dir / "**" / "*.ndjson").as_posix()
