"""
Schema-change detection and safe stop.

Source systems give no backward-compatibility guarantee, so before every ingest cycle
we diff the live source schema against the declared ``SCHEMA_CONTRACT``. Incompatible
changes stop ingestion (fail closed); additive changes only warn.

Incompatible (breaking):
- dropped column (also catches renames, which look like drop + add)
- changed data type of a contracted column
- a contracted NOT NULL column that became nullable in the source
- shrunk enum domain (an allowed value was removed)

Compatible (additive, warn only):
- brand-new columns not in the contract
- new enum values added to an existing domain
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import duckdb

from source.models import SCHEMA_CONTRACT, TableContract


class SchemaIncompatibilityError(RuntimeError):
    """Raised to stop the line when an incompatible source change is detected."""


@dataclass
class SchemaDiff:
    breaking: List[str]
    additive: List[str]


def observed_schema(
    conn: duckdb.DuckDBPyConnection, table: str
) -> Dict[str, Tuple[str, bool]]:
    """Return {column_name: (dtype, nullable)} as reported by the live source."""
    rows = conn.execute(f"DESCRIBE {table}").fetchall()
    schema: Dict[str, Tuple[str, bool]] = {}
    for row in rows:
        col_name = row[0]
        col_type = str(row[1]).upper()
        nullable = str(row[2]).upper() == "YES"
        schema[col_name] = (col_type, nullable)
    return schema


def compare_table(
    contract: TableContract,
    observed: Dict[str, Tuple[str, bool]],
    observed_enums: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> SchemaDiff:
    """Diff one table's contract against an observed schema."""
    breaking: List[str] = []
    additive: List[str] = []
    observed_enums = observed_enums or {}

    for spec in contract.columns:
        if spec.name not in observed:
            breaking.append(f"column '{spec.name}' dropped or renamed")
            continue

        obs_type, obs_nullable = observed[spec.name]
        if obs_type != spec.dtype.upper():
            breaking.append(
                f"column '{spec.name}' type changed: " f"{spec.dtype} -> {obs_type}"
            )
        if not spec.nullable and obs_nullable:
            breaking.append(
                f"column '{spec.name}' became nullable but contract requires NOT NULL"
            )

        if spec.enum is not None and spec.name in observed_enums:
            removed = set(spec.enum) - set(observed_enums[spec.name])
            added = set(observed_enums[spec.name]) - set(spec.enum)
            if removed:
                breaking.append(
                    f"column '{spec.name}' enum value(s) removed: {sorted(removed)}"
                )
            if added:
                additive.append(
                    f"column '{spec.name}' enum value(s) added: {sorted(added)}"
                )

    for col in observed:
        if contract.column(col) is None:
            additive.append(f"new column '{col}' not in contract")

    return SchemaDiff(breaking=breaking, additive=additive)


def assert_compatible(
    conn: duckdb.DuckDBPyConnection,
    contract: Dict[str, TableContract] = SCHEMA_CONTRACT,
    observed_enums: Optional[Dict[str, Dict[str, Tuple[str, ...]]]] = None,
) -> Dict[str, List[str]]:
    """
    Check every contracted table against the live source schema.

    Returns a map of table -> additive-change warnings.
    Raises ``SchemaIncompatibilityError`` if any breaking change is detected.
    """
    observed_enums = observed_enums or {}
    all_breaking: List[str] = []
    warnings: Dict[str, List[str]] = {}

    for table, table_contract in contract.items():
        try:
            observed = observed_schema(conn, table)
        except Exception as exc:  # noqa: BLE001 - surfaced as a breaking change
            all_breaking.append(f"{table}: cannot read source schema ({exc})")
            continue

        diff = compare_table(table_contract, observed, observed_enums.get(table))
        all_breaking.extend(f"{table}: {msg}" for msg in diff.breaking)
        if diff.additive:
            warnings[table] = diff.additive

    if all_breaking:
        raise SchemaIncompatibilityError(
            "Incompatible source schema change(s) detected; ingestion stopped:\n  - "
            + "\n  - ".join(all_breaking)
        )

    return warnings
