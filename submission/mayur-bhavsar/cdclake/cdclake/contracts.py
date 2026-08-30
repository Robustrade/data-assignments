"""Data contracts and the schema compatibility policy.

A contract is the pipeline's written-down belief about one source table. The
gate compares that belief against live introspection before every batch and
classifies each difference as COMPATIBLE or BREAKING using an explicit policy
matrix — never ad hoc judgement.

Two asymmetries are worth calling out, because the obvious rules are wrong:

* **Both nullability directions are breaking.** NOT NULL -> nullable does not
  break the source, but it breaks every downstream not-null assertion.
  nullable -> NOT NULL does not break readers, but it breaks writers and any
  backfill. Neither can be waved through.
* **A rename is indistinguishable from a drop plus an add.** Without a mapping
  hint from the source team there is no evidence that ``full_name`` became
  ``customer_name`` rather than vanishing. The gate says so explicitly and
  halts; the operator supplies the intent.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cdclake.config import CONTRACTS_DIR, TRACKED_TABLES

BREAKING = "BREAKING"
COMPATIBLE = "COMPATIBLE"

#: Human context carried on the published contract, so a consumer reading only
#: contracts/ still learns what the table means and how it changes.
TABLE_DESCRIPTIONS: dict[str, str] = {
    "customer": (
        "Strong entity. A person who can place orders. Changes are low volume: "
        "profile edits and status transitions."
    ),
    "product": (
        "Strong entity. Sellable item. Price edits are the interesting change "
        "pattern; they must never retroactively alter historical order lines."
    ),
    "customer_order": (
        "Strong entity. The order header. Highest-value table: status walks "
        "PENDING -> PAID -> SHIPPED -> DELIVERED, or CANCELLED from PENDING/PAID."
    ),
    "order_item": (
        "Weak entity. A line within an order; line_no is unique only within its "
        "order and the row cannot outlive the order (ON DELETE CASCADE)."
    ),
    "payment": (
        "Payment against an order. Never cascade-deleted with the order, because "
        "financial records must survive an order erasure."
    ),
    "payment_attempt": (
        "Weak entity. One gateway call for a payment. Highest-churn table in the "
        "domain; retries make CDC volume realistic."
    ),
}

#: ``<col> IN ('A', 'B')`` inside a CHECK constraint, across line breaks.
_ENUM_RE = re.compile(
    r"(?P<col>\b\w+\b)\s+IN\s*\(\s*(?P<vals>'(?:[^']*)'(?:\s*,\s*'[^']*')*)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_DECIMAL_RE = re.compile(
    r"^\s*(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.I
)


# ── contract model ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    nullable: bool
    enum: tuple[str, ...] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
        }
        if self.enum:
            out["enum"] = list(self.enum)
        return out

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ColumnSpec:
        enum = raw.get("enum")
        return cls(
            name=raw["name"],
            type=raw["type"],
            nullable=bool(raw["nullable"]),
            enum=tuple(enum) if enum else None,
        )


@dataclass(frozen=True)
class ForeignKeySpec:
    columns: tuple[str, ...]
    references_table: str
    references_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "columns": list(self.columns),
            "references_table": self.references_table,
            "references_columns": list(self.references_columns),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ForeignKeySpec:
        return cls(
            columns=tuple(raw["columns"]),
            references_table=raw["references_table"],
            references_columns=tuple(raw["references_columns"]),
        )


@dataclass(frozen=True)
class TableContract:
    table: str
    contract_version: int
    primary_key: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    foreign_keys: tuple[ForeignKeySpec, ...] = ()
    unique_constraints: tuple[tuple[str, ...], ...] = ()
    indexes: tuple[str, ...] = ()
    description: str = ""
    owner: str = "commerce-platform"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> ColumnSpec | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "contract_version": self.contract_version,
            "owner": self.owner,
            "description": self.description,
            "primary_key": list(self.primary_key),
            "columns": [c.to_dict() for c in self.columns],
            "foreign_keys": [fk.to_dict() for fk in self.foreign_keys],
            "unique_constraints": [list(u) for u in self.unique_constraints],
            "indexes": list(self.indexes),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TableContract:
        return cls(
            table=raw["table"],
            contract_version=int(raw["contract_version"]),
            primary_key=tuple(raw["primary_key"]),
            columns=tuple(ColumnSpec.from_dict(c) for c in raw["columns"]),
            foreign_keys=tuple(
                ForeignKeySpec.from_dict(f) for f in raw.get("foreign_keys", [])
            ),
            unique_constraints=tuple(
                tuple(u) for u in raw.get("unique_constraints", [])
            ),
            indexes=tuple(raw.get("indexes", [])),
            description=raw.get("description", ""),
            owner=raw.get("owner", "commerce-platform"),
        )


# ── introspection ───────────────────────────────────────────────────────────
def _enum_domains(conn: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    if row is None or row[0] is None:
        return {}
    domains: dict[str, tuple[str, ...]] = {}
    for match in _ENUM_RE.finditer(str(row[0])):
        values = tuple(v.strip().strip("'") for v in match.group("vals").split(","))
        # Convention: the unconditional domain CHECK is declared inline with the
        # column, so the first IN-list wins. Later CHECKs on the same column are
        # conditional rules (e.g. "settled payments carry a settled_at"), not
        # domain declarations, and must not narrow the published domain.
        domains.setdefault(match.group("col"), values)
    return domains


def introspect_table(
    conn: sqlite3.Connection, table: str, *, contract_version: int = 1
) -> TableContract:
    """Read the live shape of a source table. No contract file is consulted."""
    info = list(conn.execute(f"PRAGMA table_info({table})"))
    if not info:
        raise ValueError(f"table {table!r} does not exist")

    domains = _enum_domains(conn, table)
    primary_key = tuple(
        r["name"] for r in sorted((r for r in info if r["pk"]), key=lambda r: r["pk"])
    )
    # SQLite reports notnull=0 for an INTEGER PRIMARY KEY (it is a rowid alias),
    # even though such a column can never hold NULL. Publishing that verbatim
    # would drop the not-null assertion on every surrogate key downstream.
    columns = tuple(
        ColumnSpec(
            name=r["name"],
            type=(r["type"] or "").strip(),
            nullable=not bool(r["notnull"]) and r["name"] not in primary_key,
            enum=domains.get(r["name"]),
        )
        for r in info
    )

    fks: list[ForeignKeySpec] = []
    grouped: dict[int, list[sqlite3.Row]] = {}
    for r in conn.execute(f"PRAGMA foreign_key_list({table})"):
        grouped.setdefault(r["id"], []).append(r)
    for _fk_id, rows in sorted(grouped.items()):
        rows.sort(key=lambda r: r["seq"])
        fks.append(
            ForeignKeySpec(
                columns=tuple(r["from"] for r in rows),
                references_table=rows[0]["table"],
                references_columns=tuple(r["to"] for r in rows),
            )
        )

    uniques: list[tuple[str, ...]] = []
    indexes: list[str] = []
    for idx in conn.execute(f"PRAGMA index_list({table})"):
        cols = tuple(
            r["name"] for r in conn.execute(f"PRAGMA index_info({idx['name']})")
        )
        if idx["origin"] == "u":
            uniques.append(cols)
        elif idx["origin"] == "c":
            indexes.append(idx["name"])

    return TableContract(
        table=table,
        contract_version=contract_version,
        primary_key=primary_key,
        columns=columns,
        foreign_keys=tuple(fks),
        unique_constraints=tuple(sorted(uniques)),
        indexes=tuple(sorted(indexes)),
    )


def introspect_all(
    conn: sqlite3.Connection, tables: Iterable[str] = TRACKED_TABLES
) -> dict[str, TableContract]:
    return {t: introspect_table(conn, t) for t in tables}


# ── contract registry (the committed artefacts) ─────────────────────────────
def contract_path(table: str, directory: Path = CONTRACTS_DIR) -> Path:
    return directory / f"{table}.json"


def load_contract(table: str, directory: Path = CONTRACTS_DIR) -> TableContract:
    path = contract_path(table, directory)
    if not path.exists():
        raise FileNotFoundError(f"no contract published for table {table!r}: {path}")
    return TableContract.from_dict(json.loads(path.read_text(encoding="utf-8")))


def load_all(
    tables: Iterable[str] = TRACKED_TABLES, directory: Path = CONTRACTS_DIR
) -> dict[str, TableContract]:
    return {t: load_contract(t, directory) for t in tables}


def write_contract(contract: TableContract, directory: Path = CONTRACTS_DIR) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = contract_path(contract.table, directory)
    path.write_text(
        json.dumps(contract.to_dict(), indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return path


# ── compatibility policy ────────────────────────────────────────────────────
@dataclass(frozen=True)
class SchemaDelta:
    table: str
    kind: str
    severity: str
    detail: str
    column: str | None = None

    def __str__(self) -> str:
        where = f"{self.table}.{self.column}" if self.column else self.table
        return f"[{self.severity}] {self.kind} on {where}: {self.detail}"


@dataclass
class GateVerdict:
    deltas: list[SchemaDelta] = field(default_factory=list)

    @property
    def breaking(self) -> list[SchemaDelta]:
        return [d for d in self.deltas if d.severity == BREAKING]

    @property
    def warnings(self) -> list[SchemaDelta]:
        return [d for d in self.deltas if d.severity == COMPATIBLE]

    @property
    def is_compatible(self) -> bool:
        return not self.breaking

    def render(self) -> str:
        if not self.deltas:
            return "schema matches contract"
        return "\n".join(f"  {d}" for d in self.deltas)


def _decimal_parts(type_str: str) -> tuple[int, int] | None:
    match = _DECIMAL_RE.match(type_str)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _type_change_severity(expected: str, actual: str) -> tuple[str, str]:
    """Classify a declared-type change. Only decimal widening is safe."""
    exp, act = _decimal_parts(expected), _decimal_parts(actual)
    if exp and act and act[1] == exp[1] and act[0] >= exp[0]:
        return COMPATIBLE, "decimal precision widened; values still representable"
    return BREAKING, "declared type changed in a way that is not value preserving"


def diff_table(expected: TableContract, actual: TableContract) -> list[SchemaDelta]:
    """Apply the policy matrix to one table. Order of checks is stable so the
    rendered report is diffable."""
    deltas: list[SchemaDelta] = []
    exp_names, act_names = set(expected.column_names), set(actual.column_names)

    dropped = sorted(exp_names - act_names)
    added = sorted(act_names - exp_names)

    # Rename detection: exactly one column left and one arrived with the same
    # declared type. Report it as a rename *candidate* — it is still breaking.
    rename_pairs: list[tuple[str, str]] = []
    if len(dropped) == 1 and len(added) == 1:
        old_col = expected.column(dropped[0])
        new_col = actual.column(added[0])
        if old_col and new_col and old_col.type == new_col.type:
            rename_pairs.append((dropped[0], added[0]))

    if rename_pairs:
        old_name, new_name = rename_pairs[0]
        deltas.append(
            SchemaDelta(
                table=expected.table,
                kind="COLUMN_RENAMED_OR_REPLACED",
                severity=BREAKING,
                column=old_name,
                detail=(
                    f"{old_name!r} disappeared and {new_name!r} appeared with the "
                    "same declared type. A rename is indistinguishable from a "
                    "drop plus an add without a mapping hint from the source "
                    "team, so this halts and requires an explicit decision."
                ),
            )
        )
    else:
        for name in dropped:
            deltas.append(
                SchemaDelta(
                    table=expected.table,
                    kind="COLUMN_DROPPED",
                    severity=BREAKING,
                    column=name,
                    detail="column present in the contract is gone from the source",
                )
            )
        for name in added:
            col = actual.column(name)
            deltas.append(
                SchemaDelta(
                    table=expected.table,
                    kind="COLUMN_ADDED",
                    severity=COMPATIBLE,
                    column=name,
                    detail=(
                        f"new column (nullable={col.nullable if col else '?'}); "
                        "readers are unaffected, but the capture triggers must be "
                        "regenerated before the column reaches the lake"
                    ),
                )
            )

    for name in sorted(exp_names & act_names):
        exp_col = expected.column(name)
        act_col = actual.column(name)
        assert exp_col is not None and act_col is not None

        if exp_col.type != act_col.type:
            severity, detail = _type_change_severity(exp_col.type, act_col.type)
            deltas.append(
                SchemaDelta(
                    table=expected.table,
                    kind="COLUMN_TYPE_CHANGED",
                    severity=severity,
                    column=name,
                    detail=f"{exp_col.type} -> {act_col.type}: {detail}",
                )
            )

        if exp_col.nullable != act_col.nullable:
            # Both directions break something. See the module docstring.
            kind = (
                "NULLABILITY_TIGHTENED" if exp_col.nullable else "NULLABILITY_RELAXED"
            )
            detail = (
                "nullable -> NOT NULL breaks writers and any backfill of "
                "historical rows"
                if exp_col.nullable
                else "NOT NULL -> nullable breaks every downstream not-null "
                "assertion built on this column"
            )
            deltas.append(
                SchemaDelta(
                    table=expected.table,
                    kind=kind,
                    severity=BREAKING,
                    column=name,
                    detail=detail,
                )
            )

        exp_enum = set(exp_col.enum or ())
        act_enum = set(act_col.enum or ())
        if exp_enum or act_enum:
            removed = sorted(exp_enum - act_enum)
            gained = sorted(act_enum - exp_enum)
            if removed:
                deltas.append(
                    SchemaDelta(
                        table=expected.table,
                        kind="ENUM_VALUE_REMOVED",
                        severity=BREAKING,
                        column=name,
                        detail=(
                            f"domain lost {removed}; history in the lake still "
                            "contains those values, so their meaning has changed"
                        ),
                    )
                )
            if gained:
                deltas.append(
                    SchemaDelta(
                        table=expected.table,
                        kind="ENUM_VALUE_ADDED",
                        severity=COMPATIBLE,
                        column=name,
                        detail=(
                            f"domain gained {gained}; ingestion is safe but the "
                            "warehouse domain assertion will fail until the "
                            "contract is republished"
                        ),
                    )
                )

    if expected.primary_key != actual.primary_key:
        deltas.append(
            SchemaDelta(
                table=expected.table,
                kind="PRIMARY_KEY_CHANGED",
                severity=BREAKING,
                detail=(
                    f"{list(expected.primary_key)} -> {list(actual.primary_key)}; "
                    "the merge key of every downstream model is invalidated"
                ),
            )
        )

    if set(expected.foreign_keys) != set(actual.foreign_keys):
        deltas.append(
            SchemaDelta(
                table=expected.table,
                kind="FOREIGN_KEY_CHANGED",
                severity=BREAKING,
                detail="referential assumptions changed",
            )
        )

    if set(expected.unique_constraints) != set(actual.unique_constraints):
        deltas.append(
            SchemaDelta(
                table=expected.table,
                kind="UNIQUE_CONSTRAINT_CHANGED",
                severity=BREAKING,
                detail=(
                    f"{[list(u) for u in expected.unique_constraints]} -> "
                    f"{[list(u) for u in actual.unique_constraints]}"
                ),
            )
        )

    lost_indexes = sorted(set(expected.indexes) - set(actual.indexes))
    if lost_indexes:
        deltas.append(
            SchemaDelta(
                table=expected.table,
                kind="INDEX_REMOVED",
                severity=COMPATIBLE,
                detail=(
                    f"{lost_indexes} no longer exist; correctness is unaffected "
                    "but extraction latency may regress"
                ),
            )
        )

    return deltas


def diff_all(
    expected: dict[str, TableContract], actual: dict[str, TableContract]
) -> GateVerdict:
    verdict = GateVerdict()
    for table in sorted(expected):
        if table not in actual:
            verdict.deltas.append(
                SchemaDelta(
                    table=table,
                    kind="TABLE_DROPPED",
                    severity=BREAKING,
                    detail="contracted table is missing from the source",
                )
            )
            continue
        verdict.deltas.extend(diff_table(expected[table], actual[table]))
    for table in sorted(set(actual) - set(expected)):
        verdict.deltas.append(
            SchemaDelta(
                table=table,
                kind="TABLE_ADDED",
                severity=COMPATIBLE,
                detail="new source table, not yet under contract or capture",
            )
        )
    return verdict


# ── warehouse type mapping (used by the dbt codegen) ────────────────────────
def duckdb_type(sqlite_type: str) -> str:
    """Map a declared source type to the DuckDB type used in the warehouse.

    Money becomes an exact DECIMAL: SQLite stores it as a float because of
    NUMERIC affinity, and casting once on ingest is what makes downstream
    monetary arithmetic exact rather than approximately right.
    """
    normalised = sqlite_type.strip().upper()
    if _decimal_parts(normalised):
        return "DECIMAL(18,2)"
    if normalised.startswith("INT"):
        return "BIGINT"
    if normalised in {"REAL", "DOUBLE", "FLOAT"}:
        return "DOUBLE"
    return "VARCHAR"
