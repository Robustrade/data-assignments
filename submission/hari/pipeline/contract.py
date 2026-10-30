"""The versioned source schema contract.

* load the contract (YAML) into immutable dataclasses,
* introspect the *live* SQLite schema,
* diff the two (violations stop the pipeline, warnings do not),
* validate individual change events against the contract version they were stamped with.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from pipeline.clock import parse_iso

INTERNAL_TABLES = {"change_log", "_txn_context"}
TYPES = ("TEXT", "INTEGER")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False
    enum: tuple[str, ...] | None = None
    min: int | None = None
    max: int | None = None
    money: bool = False
    timestamp: bool = False
    range_rule: str = "S2"


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    references: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableCheck:
    rule: str
    expr: str


@dataclass(frozen=True)
class Index:
    name: str
    columns: tuple[str, ...]
    unique: bool = False
    where: str | None = None


@dataclass(frozen=True)
class Table:
    name: str
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()
    table_checks: tuple[TableCheck, ...] = ()
    indexes: tuple[Index, ...] = ()
    transition_column: str | None = None
    transitions: dict[str, tuple[str, ...]] = field(default_factory=dict)
    no_delete: bool = False
    append_only: bool = False
    pii: tuple[str, ...] = ()

    def column(self, name: str) -> Column:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"{self.name}.{name}")

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


@dataclass(frozen=True)
class Contract:
    version: int
    tables: dict[str, Table]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Contract:
        tables = {}
        for name, spec in data["tables"].items():
            columns = tuple(
                Column(
                    name=cname,
                    type=c["type"].upper(),
                    nullable=bool(c.get("nullable", False)),
                    enum=tuple(c["enum"]) if c.get("enum") else None,
                    min=c.get("min"),
                    max=c.get("max"),
                    money=bool(c.get("money", False)),
                    timestamp=bool(c.get("timestamp", False)),
                    range_rule=c.get("range_rule", "S2"),
                )
                for cname, c in spec["columns"].items()
            )
            trans = spec.get("transitions") or {}
            tables[name] = Table(
                name=name,
                primary_key=tuple(spec["primary_key"]),
                columns=columns,
                foreign_keys=tuple(
                    ForeignKey(
                        tuple(f["columns"]), f["references"], tuple(f["ref_columns"])
                    )
                    for f in spec.get("foreign_keys", [])
                ),
                table_checks=tuple(
                    TableCheck(c["rule"], c["expr"])
                    for c in spec.get("table_checks", [])
                ),
                indexes=tuple(
                    Index(
                        i["name"],
                        tuple(i["columns"]),
                        bool(i.get("unique", False)),
                        i.get("where"),
                    )
                    for i in spec.get("indexes", [])
                ),
                transition_column=trans.get("column"),
                transitions={k: tuple(v) for k, v in trans.get("allowed", {}).items()},
                no_delete=bool(spec.get("no_delete", False)),
                append_only=bool(spec.get("append_only", False)),
                pii=tuple(spec.get("pii", [])),
            )
        return cls(version=int(data["version"]), tables=tables)

    def to_dict(self) -> dict[str, Any]:
        tables: dict[str, Any] = {}
        for name, t in self.tables.items():
            cols = {}
            for c in t.columns:
                d: dict[str, Any] = {"type": c.type, "nullable": c.nullable}
                if c.enum:
                    d["enum"] = list(c.enum)
                if c.min is not None:
                    d["min"] = c.min
                if c.max is not None:
                    d["max"] = c.max
                if c.money:
                    d["money"] = True
                if c.timestamp:
                    d["timestamp"] = True
                if c.range_rule != "S2":
                    d["range_rule"] = c.range_rule
                cols[c.name] = d
            spec: dict[str, Any] = {
                "primary_key": list(t.primary_key),
                "columns": cols,
                "foreign_keys": [
                    {
                        "columns": list(f.columns),
                        "references": f.references,
                        "ref_columns": list(f.ref_columns),
                    }
                    for f in t.foreign_keys
                ],
                "table_checks": [
                    {"rule": c.rule, "expr": c.expr} for c in t.table_checks
                ],
                "indexes": [
                    {
                        "name": i.name,
                        "columns": list(i.columns),
                        "unique": i.unique,
                        **({"where": i.where} if i.where else {}),
                    }
                    for i in t.indexes
                ],
                "no_delete": t.no_delete,
                "append_only": t.append_only,
                "pii": list(t.pii),
            }
            if t.transition_column:
                spec["transitions"] = {
                    "column": t.transition_column,
                    "allowed": {k: list(v) for k, v in t.transitions.items()},
                }
            tables[name] = spec
        return {"version": self.version, "tables": tables}


def load_contract(path: str | Path) -> Contract:
    with open(path, encoding="utf-8") as handle:
        return Contract.from_dict(yaml.safe_load(handle))


# --------------------------------------------------------------------------- live schema


@dataclass
class LiveTable:
    name: str
    columns: dict[str, tuple[str, bool]]  # name -> (declared type, not null)
    primary_key: tuple[str, ...]
    foreign_keys: set[tuple[tuple[str, ...], str, tuple[str, ...]]]
    enums: dict[str, frozenset[str]]
    triggers: set[str]


_ENUM_RE = re.compile(
    r"CHECK\s*\(\s*\"?(\w+)\"?\s+IN\s*\(([^)]*)\)\s*\)", re.IGNORECASE
)


def parse_enums(create_sql: str) -> dict[str, frozenset[str]]:
    """Extract ``CHECK (col IN ('a','b'))`` domains from a CREATE TABLE statement."""
    enums: dict[str, frozenset[str]] = {}
    for column, body in _ENUM_RE.findall(create_sql or ""):
        enums[column] = frozenset(re.findall(r"'([^']*)'", body))
    return enums


def introspect(conn) -> dict[str, LiveTable]:
    """Read the live SQLite schema of the business tables (and their triggers)."""
    live: dict[str, LiveTable] = {}
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    triggers: dict[str, set[str]] = {}
    for name, tbl in conn.execute(
        "SELECT name, tbl_name FROM sqlite_master WHERE type = 'trigger'"
    ).fetchall():
        triggers.setdefault(tbl, set()).add(name)
    for name, sql in rows:
        if name in INTERNAL_TABLES:
            continue
        info = conn.execute(f'PRAGMA table_info("{name}")').fetchall()
        columns = {r[1]: (str(r[2]).upper(), bool(r[3])) for r in info}
        pk = tuple(r[1] for r in sorted((r for r in info if r[5]), key=lambda r: r[5]))
        fks: dict[int, dict[str, Any]] = {}
        for r in conn.execute(f'PRAGMA foreign_key_list("{name}")').fetchall():
            fk = fks.setdefault(r[0], {"table": r[2], "from": [], "to": []})
            fk["from"].append((r[3], r[1]))
            fk["to"].append((r[4], r[1]))
        fk_set = set()
        for fk in fks.values():
            pairs = sorted(fk["from"], key=lambda p: p[1])
            fk_set.add(
                (
                    tuple(p[0] for p in pairs),
                    fk["table"],
                    tuple(t[0] for t in sorted(fk["to"], key=lambda p: p[1])),
                )
            )
        live[name] = LiveTable(
            name, columns, pk, fk_set, parse_enums(sql), triggers.get(name, set())
        )
    return live


def cdc_trigger_names(table: str) -> list[str]:
    return [f"cdc_{table}_ins", f"cdc_{table}_upd", f"cdc_{table}_del"]


def _change(kind: str, table: str, column: str | None, expected, actual, message: str):
    return {
        "kind": kind,
        "table": table,
        "column": column,
        "expected": expected,
        "actual": actual,
        "message": message,
    }


def diff_live(
    contract: Contract, live: dict[str, LiveTable]
) -> tuple[list[dict], list[dict]]:
    """Compare the live schema with a contract. Returns ``(violations, warnings)``."""
    violations: list[dict] = []
    warnings: list[dict] = []
    for tname, table in contract.tables.items():
        if tname not in live:
            violations.append(
                _change(
                    "table_dropped",
                    tname,
                    None,
                    "present",
                    "missing",
                    f"table {tname} is missing",
                )
            )
            continue
        lt = live[tname]
        expected_cols = {c.name: c for c in table.columns}
        for cname, col in expected_cols.items():
            if cname not in lt.columns:
                violations.append(
                    _change(
                        "column_missing",
                        tname,
                        cname,
                        "present",
                        "missing",
                        f"column {tname}.{cname} is missing (dropped or renamed)",
                    )
                )
                continue
            ltype, lnotnull = lt.columns[cname]
            if ltype != col.type:
                violations.append(
                    _change(
                        "type_changed",
                        tname,
                        cname,
                        col.type,
                        ltype,
                        f"column {tname}.{cname} type {col.type} -> {ltype}",
                    )
                )
            if lnotnull != (not col.nullable):
                violations.append(
                    _change(
                        "nullability_changed",
                        tname,
                        cname,
                        "NULL" if col.nullable else "NOT NULL",
                        "NOT NULL" if lnotnull else "NULL",
                        f"column {tname}.{cname} nullability changed",
                    )
                )
            want = frozenset(col.enum) if col.enum else None
            have = lt.enums.get(cname)
            if want != have:
                violations.append(
                    _change(
                        "enum_changed",
                        tname,
                        cname,
                        sorted(want) if want else None,
                        sorted(have) if have else None,
                        f"column {tname}.{cname} allowed values changed",
                    )
                )
        for cname, (ltype, lnotnull) in lt.columns.items():
            if cname in expected_cols:
                continue
            item = _change(
                "column_added",
                tname,
                cname,
                None,
                ltype,
                f"new column {tname}.{cname} is not in the contract",
            )
            (violations if lnotnull else warnings).append(item)
        if lt.primary_key != table.primary_key:
            violations.append(
                _change(
                    "primary_key_changed",
                    tname,
                    None,
                    list(table.primary_key),
                    list(lt.primary_key),
                    f"primary key of {tname} changed",
                )
            )
        want_fks = {
            (f.columns, f.references, f.ref_columns) for f in table.foreign_keys
        }
        if want_fks != lt.foreign_keys:
            violations.append(
                _change(
                    "foreign_key_changed",
                    tname,
                    None,
                    sorted(map(str, want_fks)),
                    sorted(map(str, lt.foreign_keys)),
                    f"foreign keys of {tname} changed",
                )
            )
        for trig in cdc_trigger_names(tname):
            if trig not in lt.triggers:
                violations.append(
                    _change(
                        "trigger_missing",
                        tname,
                        None,
                        trig,
                        "missing",
                        f"CDC trigger {trig} is missing",
                    )
                )
    for tname in live:
        if tname not in contract.tables:
            warnings.append(
                _change(
                    "table_added",
                    tname,
                    None,
                    None,
                    "present",
                    f"new table {tname} is not in the contract",
                )
            )
    return violations, warnings


def diff_contracts(old: Contract | None, new: Contract) -> list[dict]:
    """Describe what changed between two contract versions (stored in the registry)."""
    changes: list[dict] = []
    old_tables = old.tables if old else {}
    for tname, table in new.tables.items():
        if tname not in old_tables:
            changes.append(
                _change(
                    "table_added", tname, None, None, "present", f"table {tname} added"
                )
            )
            continue
        old_cols = {c.name: c for c in old_tables[tname].columns}
        new_cols = {c.name: c for c in table.columns}
        for cname, col in new_cols.items():
            if cname not in old_cols:
                changes.append(
                    _change(
                        "column_added",
                        tname,
                        cname,
                        None,
                        col.type,
                        f"column {tname}.{cname} added",
                    )
                )
            elif old_cols[cname] != col:
                changes.append(
                    _change(
                        "column_changed",
                        tname,
                        cname,
                        old_cols[cname].type,
                        col.type,
                        f"column {tname}.{cname} changed",
                    )
                )
        for cname, old_col in old_cols.items():
            if cname not in new_cols:
                changes.append(
                    _change(
                        "column_dropped",
                        tname,
                        cname,
                        old_col.type,
                        None,
                        f"column {tname}.{cname} dropped",
                    )
                )
    for tname in old_tables:
        if tname not in new.tables:
            changes.append(
                _change(
                    "table_dropped",
                    tname,
                    None,
                    "present",
                    None,
                    f"table {tname} dropped",
                )
            )
    return changes


# --------------------------------------------------------------------------- events


def check_event_values(contract: Contract, event) -> list[dict]:
    """Validate one event's row image against the contract it was captured under."""
    table = contract.tables.get(event.table_name)
    if table is None:
        return [
            {"seq": event.seq, "message": f"event for unknown table {event.table_name}"}
        ]
    image = event.before if event.op == "delete" else event.after
    if image is None:
        return [{"seq": event.seq, "message": "event has no row image"}]
    problems: list[dict] = []
    for col in table.columns:
        value = image.get(col.name)
        where = f"{event.table_name}.{col.name} (seq {event.seq})"
        if value is None:
            if not col.nullable:
                problems.append(
                    {"seq": event.seq, "message": f"{where}: NULL not allowed"}
                )
            continue
        if col.type == "INTEGER":
            ok = isinstance(value, int) and not isinstance(value, bool)
        else:
            ok = isinstance(value, str)
        if not ok:
            problems.append(
                {
                    "seq": event.seq,
                    "message": f"{where}: expected {col.type}, got {type(value).__name__}",
                }
            )
            continue
        if col.enum and value not in col.enum:
            problems.append(
                {
                    "seq": event.seq,
                    "message": f"{where}: value {value!r} not in {list(col.enum)}",
                }
            )
        if col.timestamp:
            try:
                parse_iso(value)
            except ValueError:
                problems.append(
                    {"seq": event.seq, "message": f"{where}: bad timestamp {value!r}"}
                )
    return problems
