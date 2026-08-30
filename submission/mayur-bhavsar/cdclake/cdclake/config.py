"""Paths, constants and the workspace abstraction.

Every runtime artefact (source database, lake files, warehouse database,
pipeline state, metrics) lives under a single ``Workspace`` root.  Nothing is
written to the repository tree at runtime, which is what makes the whole
pipeline safe to instantiate many times over inside tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SQL_DIR = ROOT / "sql"
CONTRACTS_DIR = ROOT / "contracts"
CATALOG_FILE = ROOT / "catalog" / "datasets.yml"
DBT_PROJECT_DIR = ROOT
FIXTURE_LAKE_FILE = ROOT / "warehouse" / "fixtures" / "lake_cdc_events.ndjson"
DEFAULT_WORKSPACE = ROOT / ".work"

#: Source tables under change data capture, in foreign-key dependency order.
TRACKED_TABLES: tuple[str, ...] = (
    "customer",
    "product",
    "customer_order",
    "order_item",
    "payment",
    "payment_attempt",
)

#: Entities whose identity depends on a parent row (weak entities).
WEAK_ENTITIES: frozenset[str] = frozenset({"order_item", "payment_attempt"})

OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"
VALID_OPS = frozenset({OP_INSERT, OP_UPDATE, OP_DELETE})

#: Millisecond precision, matching SQLite's strftime('%Y-%m-%dT%H:%M:%fZ') byte
#: for byte so that timestamps written by the triggers and by Python sort and
#: compare identically. %f parses 1-6 digits, so it round-trips either width.
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def utcnow_iso() -> str:
    """UTC timestamp in the single format used everywhere in this project."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, TIMESTAMP_FORMAT).replace(tzinfo=UTC)


@dataclass(frozen=True)
class Workspace:
    """Filesystem layout for one instance of the platform."""

    root: Path

    @classmethod
    def create(cls, root: Path | str | None = None) -> Workspace:
        ws = cls(Path(root or DEFAULT_WORKSPACE).resolve())
        ws.ensure()
        return ws

    def ensure(self) -> None:
        for path in (self.root, self.lake_dir, self.state_dir, self.warehouse_dir):
            path.mkdir(parents=True, exist_ok=True)

    # ── source ──────────────────────────────────────────────────────────────
    @property
    def source_db(self) -> Path:
        return self.root / "source.db"

    # ── lake ────────────────────────────────────────────────────────────────
    @property
    def lake_dir(self) -> Path:
        return self.root / "lake"

    @property
    def lake_events_dir(self) -> Path:
        return self.lake_dir / "cdc_events"

    @property
    def manifest_path(self) -> Path:
        return self.lake_dir / "_manifest" / "batches.ndjson"

    # ── control plane ───────────────────────────────────────────────────────
    @property
    def state_dir(self) -> Path:
        return self.root / "_state"

    @property
    def state_path(self) -> Path:
        return self.state_dir / "pipeline_state.json"

    @property
    def metrics_path(self) -> Path:
        return self.state_dir / "metrics.ndjson"

    @property
    def alerts_path(self) -> Path:
        return self.state_dir / "alerts.ndjson"

    # ── warehouse ───────────────────────────────────────────────────────────
    @property
    def warehouse_dir(self) -> Path:
        return self.root / "warehouse"

    @property
    def warehouse_db(self) -> Path:
        return self.warehouse_dir / "warehouse.duckdb"
