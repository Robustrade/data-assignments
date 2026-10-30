"""Warehouse tables generated from the contract(s).

Each source column maps to one warehouse column:

* ``*_paise`` money -> same name without the suffix, ``DECIMAL(18,2)`` rupees
* ``timestamp`` columns -> ``TIMESTAMP`` (naive UTC)
* PII columns -> ``<col>_hash`` (SHA-256 of ``lower(trim(value))``)

History is never rewritten when the contract changes: a new column is added (NULL for
older versions), a dropped column stays and is NULL from then on, and a retyped column
becomes a new column ``<col>_v<N>``.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from pipeline.clock import parse_iso
from pipeline.contract import Column, Contract, Table

SQL_TYPES = {
    "text": "VARCHAR",
    "int": "BIGINT",
    "money": "DECIMAL(18,2)",
    "ts": "TIMESTAMP",
    "pii": "VARCHAR",
}

CURRENT_META = [
    ("_last_seq", "BIGINT NOT NULL"),
    ("_last_txn_id", "VARCHAR"),
    ("_schema_version", "INTEGER"),
    ("_is_deleted", "BOOLEAN NOT NULL"),
    ("_updated_at", "TIMESTAMP"),
]
HISTORY_META = [
    ("valid_from", "TIMESTAMP"),
    ("valid_to", "TIMESTAMP"),
    ("is_current", "BOOLEAN"),
    ("op", "VARCHAR"),
    ("seq", "BIGINT NOT NULL"),
    ("txn_id", "VARCHAR"),
    ("_schema_version", "INTEGER"),
]


def column_kind(table: Table, col: Column) -> str:
    if col.name in table.pii:
        return "pii"
    if col.money:
        return "money"
    if col.timestamp:
        return "ts"
    return "int" if col.type == "INTEGER" else "text"


def base_name(col: Column, kind: str) -> str:
    if kind == "pii":
        return f"{col.name}_hash"
    if kind == "money" and col.name.endswith("_paise"):
        return col.name[: -len("_paise")]
    return col.name


@dataclass(frozen=True)
class WhColumn:
    name: str
    source: str
    kind: str
    since: int
    until: int | None = None

    @property
    def sql_type(self) -> str:
        return SQL_TYPES[self.kind]

    def active(self, version: int) -> bool:
        return self.since <= version and (self.until is None or version < self.until)


@dataclass
class TableModel:
    name: str
    source_pk: tuple[str, ...]
    columns: list[WhColumn] = field(default_factory=list)

    @property
    def pk(self) -> list[str]:
        """Warehouse names of the primary-key columns."""
        by_source = {c.source: c.name for c in self.columns if c.until is None}
        return [by_source[p] for p in self.source_pk]

    def active_columns(self, version: int) -> list[WhColumn]:
        return [c for c in self.columns if c.active(version)]

    def current_columns(self) -> list[WhColumn]:
        return [c for c in self.columns if c.until is None]

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]


class WarehouseModel:
    def __init__(self, tables: dict[str, TableModel]):
        self.tables = tables

    def current_name(self, table: str) -> str:
        return f"warehouse.{table}_current"

    def history_name(self, table: str) -> str:
        return f"warehouse.{table}_history"


def build_model(contracts: list[Contract]) -> WarehouseModel:
    """Fold contract versions (ascending) into the union warehouse model."""
    tables: dict[str, TableModel] = {}
    active: dict[str, dict[str, int]] = {}  # table -> source col -> index in columns
    for contract in sorted(contracts, key=lambda c: c.version):
        v = contract.version
        for tname, table in contract.tables.items():
            tm = tables.setdefault(tname, TableModel(tname, table.primary_key))
            live = active.setdefault(tname, {})
            seen = set()
            for col in table.columns:
                kind = column_kind(table, col)
                seen.add(col.name)
                if col.name in live:
                    cur = tm.columns[live[col.name]]
                    if cur.kind == kind:
                        continue
                    tm.columns[live[col.name]] = WhColumn(
                        cur.name, cur.source, cur.kind, cur.since, v
                    )
                    name = f"{base_name(col, kind)}_v{v}"
                else:
                    name = base_name(col, kind)
                    if name in tm.names:
                        name = f"{name}_v{v}"
                tm.columns.append(WhColumn(name, col.name, kind, v))
                live[col.name] = len(tm.columns) - 1
            for source in list(live):
                if source not in seen:
                    cur = tm.columns[live.pop(source)]
                    idx = tm.columns.index(cur)
                    tm.columns[idx] = WhColumn(
                        cur.name, cur.source, cur.kind, cur.since, v
                    )
    return WarehouseModel(tables)


# ------------------------------------------------------------------ DDL


def _create_current(tm: TableModel) -> str:
    cols = [f"{c.name} {c.sql_type}" for c in tm.columns]
    cols += [f"{n} {t}" for n, t in CURRENT_META]
    return f"CREATE TABLE IF NOT EXISTS warehouse.{tm.name}_current ({', '.join(cols)})"


def _create_history(tm: TableModel) -> str:
    cols = [f"{c.name} {c.sql_type}" for c in tm.columns]
    cols += [f"{n} {t}" for n, t in HISTORY_META]
    return f"CREATE TABLE IF NOT EXISTS warehouse.{tm.name}_history ({', '.join(cols)})"


def _create_view(tm: TableModel) -> str:
    cols = ", ".join(tm.names)
    return (
        f"CREATE OR REPLACE VIEW warehouse.{tm.name} AS "
        f"SELECT {cols} FROM warehouse.{tm.name}_current WHERE NOT _is_deleted"
    )


def ensure_tables(conn, model: WarehouseModel) -> list[str]:
    """Create missing tables / columns and refresh the consumer views (idempotent)."""
    changes = []
    for tm in model.tables.values():
        existing = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'warehouse'"
            ).fetchall()
        }
        conn.execute(f"DROP VIEW IF EXISTS warehouse.{tm.name}")
        for suffix, ddl in (
            ("current", _create_current(tm)),
            ("history", _create_history(tm)),
        ):
            table = f"{tm.name}_{suffix}"
            if table not in existing:
                conn.execute(ddl)
                changes.append(f"create {table}")
                continue
            have = {
                r[0]
                for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'warehouse' AND table_name = ?",
                    [table],
                ).fetchall()
            }
            for col in tm.columns:
                if col.name not in have:
                    conn.execute(
                        f"ALTER TABLE warehouse.{table} ADD COLUMN {col.name} {col.sql_type}"
                    )
                    changes.append(f"add {table}.{col.name}")
        conn.execute(_create_view(tm))
    return changes


# ------------------------------------------------------------------ value conversion


def to_naive(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(tzinfo=None)


def hash_pii(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


def convert(value, kind: str):
    """Convert a source value into its warehouse representation."""
    if value is None:
        return None
    if kind == "money":
        return (Decimal(int(value)) / Decimal(100)).quantize(Decimal("0.01"))
    if kind == "ts":
        return to_naive(parse_iso(value))
    if kind == "pii":
        return hash_pii(value)
    return value
