"""Quality layer — the checks that dbt cannot express about itself.

dbt owns the in-warehouse assertions (127 of them). This module owns the
platform-level ones:

* **Reconciliation** — source row counts against warehouse current state.
* **Completeness** — every source change is represented in the lake, and every
  lake file the manifest vouches for still holds the rows it claimed.
* **Checkpoint integrity** — batches chain without a gap.
* **Freshness** — observed end-to-end lag against a stated objective.
* **Validation parity** — the important one: every constraint published in a
  contract has a matching assertion in models/schema.yml. This is what stops
  the two halves of "validation parity" from quietly drifting apart, and it is
  the check that fails when someone adds a source constraint and forgets the
  downstream twin.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from cdclake import contracts as contracts_mod
from cdclake.config import ROOT, TRACKED_TABLES
from cdclake.lake import read_ndjson

SCHEMA_YML = ROOT / "models" / "schema.yml"

#: Service level objective for end-to-end freshness, in seconds. Stated so that
#: "near real time" is a number somebody can be held to rather than an adjective.
FRESHNESS_SLO_SECONDS = 60.0


@dataclass
class CheckResult:
    name: str
    category: str
    passed: bool
    detail: str

    def __str__(self) -> str:
        return f"[{'PASS' if self.passed else 'FAIL'}] {self.name}: {self.detail}"


# ── schema.yml introspection ────────────────────────────────────────────────
def _test_entries(raw: Any) -> list[tuple[str, dict[str, Any]]]:
    """Normalise a dbt ``data_tests`` list into (test_name, arguments) pairs."""
    entries: list[tuple[str, dict[str, Any]]] = []
    for item in raw or []:
        if isinstance(item, str):
            entries.append((item, {}))
        elif isinstance(item, dict):
            for name, body in item.items():
                body = body or {}
                entries.append((name, body.get("arguments", body)))
    return entries


def load_declared_tests(path: Path = SCHEMA_YML) -> dict[str, dict[str, list[tuple]]]:
    """model -> column -> [(test_name, arguments), ...]"""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    out: dict[str, dict[str, list[tuple]]] = {}
    for model in raw.get("models", []):
        columns: dict[str, list[tuple]] = {}
        for column in model.get("columns", []) or []:
            columns[column["name"]] = _test_entries(column.get("data_tests"))
        out[model["name"]] = columns
    return out


def _tests_for(columns: dict[str, list[tuple]], column: str) -> set[str]:
    return {name for name, _ in columns.get(column, [])}


def _arguments_for(
    columns: dict[str, list[tuple]], column: str, test: str
) -> dict[str, Any]:
    for name, args in columns.get(column, []):
        if name == test:
            return args
    return {}


def check_validation_parity(schema_path: Path = SCHEMA_YML) -> list[CheckResult]:
    """Every published source constraint must have a warehouse assertion."""
    declared = load_declared_tests(schema_path)
    results: list[CheckResult] = []

    for table in TRACKED_TABLES:
        contract = contracts_mod.load_contract(table)
        model = f"{table}_current"
        columns = declared.get(model)
        if columns is None:
            results.append(
                CheckResult(
                    f"parity::{model}",
                    "validation-parity",
                    False,
                    f"no assertions declared for {model}",
                )
            )
            continue

        # Surrogate identity: the merge key is always asserted unique.
        missing_key = {"not_null", "unique"} - _tests_for(columns, "pk_hash")
        results.append(
            CheckResult(
                f"parity::{model}.pk_hash",
                "validation-parity",
                not missing_key,
                (
                    "pk_hash asserted not_null + unique"
                    if not missing_key
                    else f"missing {sorted(missing_key)} on pk_hash"
                ),
            )
        )

        # NOT NULL parity.
        for column in contract.columns:
            if column.nullable:
                continue
            has = "not_null" in _tests_for(columns, column.name)
            results.append(
                CheckResult(
                    f"parity::{model}.{column.name}.not_null",
                    "validation-parity",
                    has,
                    (
                        "NOT NULL mirrored"
                        if has
                        else f"source declares {column.name} NOT NULL; "
                        f"{model} has no not_null assertion"
                    ),
                )
            )

        # Enum domain parity — the exact set, not merely the presence of a test.
        for column in contract.columns:
            if not column.enum:
                continue
            args = _arguments_for(columns, column.name, "accepted_values")
            declared_values = set(args.get("values", []))
            expected = set(column.enum)
            ok = declared_values == expected
            results.append(
                CheckResult(
                    f"parity::{model}.{column.name}.accepted_values",
                    "validation-parity",
                    ok,
                    (
                        f"domain {sorted(expected)} mirrored"
                        if ok
                        else f"contract domain {sorted(expected)} != "
                        f"asserted {sorted(declared_values)}"
                    ),
                )
            )

        # UNIQUE parity for single-column source unique constraints.
        for unique in contract.unique_constraints:
            if len(unique) != 1:
                continue
            column = unique[0]
            has = "unique" in _tests_for(columns, column)
            results.append(
                CheckResult(
                    f"parity::{model}.{column}.unique",
                    "validation-parity",
                    has,
                    (
                        "UNIQUE mirrored"
                        if has
                        else f"source declares {column} UNIQUE; no unique assertion"
                    ),
                )
            )

        # Single-column primary keys are also asserted unique by name.
        if len(contract.primary_key) == 1:
            column = contract.primary_key[0]
            has = "unique" in _tests_for(columns, column)
            results.append(
                CheckResult(
                    f"parity::{model}.{column}.pk_unique",
                    "validation-parity",
                    has,
                    (
                        "primary key mirrored"
                        if has
                        else f"primary key {column} has no unique assertion"
                    ),
                )
            )

        # Referential integrity parity.
        for fk in contract.foreign_keys:
            if len(fk.columns) != 1:
                continue
            column = fk.columns[0]
            args = _arguments_for(columns, column, "relationships")
            target = str(args.get("to", ""))
            expected_target = f"ref('{fk.references_table}_current')"
            ok = (
                target.replace('"', "'") == expected_target
                and str(args.get("field", "")) == fk.references_columns[0]
            )
            results.append(
                CheckResult(
                    f"parity::{model}.{column}.relationships",
                    "validation-parity",
                    ok,
                    (
                        f"FK -> {fk.references_table} mirrored"
                        if ok
                        else f"source FK {column} -> "
                        f"{fk.references_table}.{fk.references_columns[0]} has no "
                        "matching relationships assertion"
                    ),
                )
            )

    return results


# ── platform checks ─────────────────────────────────────────────────────────
def check_reconciliation(platform: Any) -> list[CheckResult]:
    from cdclake.pipeline import reconcile

    results = []
    for table, counts in reconcile(platform).items():
        results.append(
            CheckResult(
                f"reconciliation::{table}",
                "completeness",
                counts["delta"] == 0,
                f"source={counts['source']} warehouse={counts['warehouse']} "
                f"delta={counts['delta']}",
            )
        )
    return results


def check_lake_completeness(platform: Any) -> list[CheckResult]:
    placeholders = ", ".join("?" for _ in TRACKED_TABLES)
    source_events = platform.conn.execute(
        f"SELECT count(*) FROM cdc_events WHERE table_name IN ({placeholders})",  # noqa: S608  # nosec B608
        TRACKED_TABLES,
    ).fetchone()[0]
    distinct_in_lake = {
        (record["table_name"], record["event_id"]) for record in platform.lake.records()
    }
    ok = len(distinct_in_lake) == source_events
    return [
        CheckResult(
            "lake::completeness",
            "completeness",
            ok,
            f"{len(distinct_in_lake)} distinct change(s) in the lake vs "
            f"{source_events} at the source",
        )
    ]


def check_lake_integrity(platform: Any) -> list[CheckResult]:
    """Manifest entries must still describe the files on disk exactly."""
    results = []
    for batch in platform.lake.batches():
        for entry in batch["files"]:
            path = platform.workspace.lake_dir / entry["path"]
            if not path.exists():
                results.append(
                    CheckResult(
                        f"lake::file::{entry['path']}",
                        "durability",
                        False,
                        "file referenced by the manifest is missing",
                    )
                )
                continue
            rows = sum(1 for _ in read_ndjson(path))
            results.append(
                CheckResult(
                    f"lake::file::{entry['path']}",
                    "durability",
                    rows == entry["rows"],
                    f"{rows} row(s) on disk, manifest claims {entry['rows']}",
                )
            )
    return results


def check_checkpoint_chain(platform: Any) -> list[CheckResult]:
    previous = 0
    for batch in platform.lake.batches():
        if batch["checkpoint_before"] != previous:
            return [
                CheckResult(
                    "checkpoint::chain",
                    "durability",
                    False,
                    f"batch {batch['batch_id']} starts at "
                    f"{batch['checkpoint_before']}, previous ended at {previous}",
                )
            ]
        previous = batch["checkpoint_after"]
    return [
        CheckResult(
            "checkpoint::chain",
            "durability",
            True,
            f"{len(platform.lake.batches())} batch(es) chain without a gap; "
            f"checkpoint {previous}",
        )
    ]


def check_freshness(platform: Any) -> list[CheckResult]:
    metrics = list(read_ndjson(platform.workspace.metrics_path))
    if not metrics:
        return [
            CheckResult("freshness::lag", "freshness", False, "no batch metrics found")
        ]
    worst = max(float(m["lag_max_seconds"]) for m in metrics)
    return [
        CheckResult(
            "freshness::lag",
            "freshness",
            worst <= FRESHNESS_SLO_SECONDS,
            f"worst observed source-to-lake lag {worst:.3f}s "
            f"(SLO {FRESHNESS_SLO_SECONDS:.0f}s)",
        )
    ]


def check_source_constraints(platform: Any) -> list[CheckResult]:
    """The source's own integrity, checked the way the source declares it."""
    violations = platform.conn.execute("PRAGMA foreign_key_check").fetchall()
    results = [
        CheckResult(
            "source::foreign_keys",
            "system",
            not violations,
            "no orphaned rows" if not violations else f"{len(violations)} violation(s)",
        )
    ]
    integrity = platform.conn.execute("PRAGMA integrity_check").fetchone()[0]
    results.append(
        CheckResult("source::integrity", "system", integrity == "ok", str(integrity))
    )
    return results


def run_platform_checks(platform: Any) -> list[CheckResult]:
    return [
        *check_source_constraints(platform),
        *check_reconciliation(platform),
        *check_lake_completeness(platform),
        *check_lake_integrity(platform),
        *check_checkpoint_chain(platform),
        *check_freshness(platform),
        *check_validation_parity(),
    ]


def summarise(results: list[CheckResult]) -> tuple[str, bool]:
    by_category: dict[str, list[CheckResult]] = {}
    for result in results:
        by_category.setdefault(result.category, []).append(result)

    lines: list[str] = []
    ok = True
    for category in sorted(by_category):
        checks = by_category[category]
        failures = [c for c in checks if not c.passed]
        ok = ok and not failures
        lines.append(
            f"\n{category}  ({len(checks) - len(failures)}/{len(checks)} passed)"
        )
        for check in checks:
            if check.passed:
                lines.append(f"  OK   {check.name}: {check.detail}")
            else:
                lines.append(f"  FAIL {check.name}: {check.detail}")
    return "\n".join(lines), ok


__all__ = [
    "CheckResult",
    "FRESHNESS_SLO_SECONDS",
    "check_validation_parity",
    "run_platform_checks",
    "summarise",
]
