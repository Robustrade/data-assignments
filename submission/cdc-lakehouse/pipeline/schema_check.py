"""Schema contract validation — source-owned contract vs live MySQL on every ingest."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

from pipeline.config import (
    CONTRACTS_DIR,
    FAILED_SCHEMA_PATH,
    SCHEMA_CONTRACT_PATH,
    SCHEMA_CONTRACT_SPEC_PATH,
    SCHEMA_FINGERPRINT_PATH,
)

# Legacy column-list contract (CI / DuckDB tests)
from source.models import SCHEMA_CONTRACT


def load_source_contract_spec() -> dict[str, Any]:
    """Load the source-team schema contract (authoritative)."""
    path = SCHEMA_CONTRACT_SPEC_PATH
    if not path.exists():
        raise FileNotFoundError(f"Source contract missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def contract_spec_to_information_schema_rows(spec: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert contract spec to INFORMATION_SCHEMA-shaped rows for fingerprinting."""
    rows: list[dict[str, Any]] = []
    for table, table_def in spec["tables"].items():
        for col in table_def["columns"]:
            rows.append(
                {
                    "TABLE_NAME": table,
                    "COLUMN_NAME": col["name"],
                    "DATA_TYPE": col["data_type"],
                    "IS_NULLABLE": "YES" if col["nullable"] else "NO",
                    "COLUMN_KEY": col.get("column_key", ""),
                }
            )
    return rows


def _fingerprint_from_columns(columns: list[dict[str, Any]]) -> dict[str, str]:
    """Structural hash from column name, type, and nullability (not incidental indexes)."""
    by_table: dict[str, list[str]] = {}
    for col in columns:
        table = col["table_name"] if "table_name" in col else col["TABLE_NAME"]
        name = col["column_name"] if "column_name" in col else col["COLUMN_NAME"]
        dtype = (col["data_type"] if "data_type" in col else col["DATA_TYPE"]).lower()
        nullable = col["is_nullable"] if "is_nullable" in col else col["IS_NULLABLE"]
        by_table.setdefault(table, []).append(f"{name}:{dtype}:{nullable}")
    return {
        t: hashlib.sha256("|".join(parts).encode()).hexdigest()
        for t, parts in sorted(by_table.items())
    }


def fingerprint_from_mysql(columns: list[dict[str, Any]]) -> dict[str, str]:
    normalized = [
        {
            "TABLE_NAME": c["TABLE_NAME"] if "TABLE_NAME" in c else c["table_name"],
            "COLUMN_NAME": c["COLUMN_NAME"] if "COLUMN_NAME" in c else c["column_name"],
            "DATA_TYPE": c["DATA_TYPE"] if "DATA_TYPE" in c else c["data_type"],
            "IS_NULLABLE": c["IS_NULLABLE"] if "IS_NULLABLE" in c else c["is_nullable"],
            "COLUMN_KEY": c.get("COLUMN_KEY") or c.get("column_key") or "",
        }
        for c in columns
    ]
    return _fingerprint_from_columns(normalized)


def fingerprint_from_duckdb(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    columns: list[dict[str, Any]] = []
    for table in SCHEMA_CONTRACT:
        rows = conn.execute(f"DESCRIBE {table}").fetchall()
        for row in rows:
            columns.append(
                {
                    "table_name": table,
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_nullable": row[2],
                    "column_key": row[3] if len(row) > 3 else "",
                }
            )
    return _fingerprint_from_columns(columns)


def expected_fingerprint_from_contract() -> dict[str, str]:
    """Expected fingerprint derived from source contract — not from live DB."""
    spec = load_source_contract_spec()
    return fingerprint_from_mysql(contract_spec_to_information_schema_rows(spec))


def load_expected_fingerprint() -> dict[str, str]:
    """Load committed expected fingerprint (compiled from source contract)."""
    if SCHEMA_FINGERPRINT_PATH.exists():
        return json.loads(SCHEMA_FINGERPRINT_PATH.read_text(encoding="utf-8"))
    # Fallback: compute from contract spec at validation time
    return expected_fingerprint_from_contract()


def check_contract_columns(actual_by_table: dict[str, set[str]]) -> list[str]:
    violations: list[str] = []
    for table, expected_cols in SCHEMA_CONTRACT.items():
        actual = actual_by_table.get(table, set())
        if not actual:
            violations.append(f"{table}: table missing from source")
            continue
        for col in expected_cols:
            if col not in actual:
                violations.append(f"{table}.{col}: column missing from source table")
    return violations


def validate_live_against_source_contract(
    info_schema_columns: list[dict[str, Any]],
) -> list[str]:
    """
    Validate live source schema against the source-team contract on every ingestion.

    Checks: required tables/columns, types, nullability, keys, no unexpected columns,
    and structural fingerprint match to expected contract fingerprint.
    """
    spec = load_source_contract_spec()
    violations: list[str] = []

    live_by_table: dict[str, dict[str, dict[str, Any]]] = {}
    for col in info_schema_columns:
        table = col.get("TABLE_NAME") or col.get("table_name")
        name = col.get("COLUMN_NAME") or col.get("column_name")
        live_by_table.setdefault(table, {})[name] = {
            "data_type": (col.get("DATA_TYPE") or col.get("data_type") or "").lower(),
            "is_nullable": col.get("IS_NULLABLE") or col.get("is_nullable"),
            "column_key": col.get("COLUMN_KEY") or col.get("column_key") or "",
        }

    for table, table_def in spec["tables"].items():
        live_cols = live_by_table.get(table)
        if not live_cols:
            violations.append(f"{table}: table missing from live source (contract v{spec.get('version')})")
            continue

        contract_names = {c["name"] for c in table_def["columns"]}
        live_names = set(live_cols.keys())

        for extra in sorted(live_names - contract_names):
            violations.append(
                f"{table}.{extra}: unexpected column in live source (not in source contract)"
            )

        for col_spec in table_def["columns"]:
            name = col_spec["name"]
            if name not in live_cols:
                violations.append(f"{table}.{name}: column missing from live source")
                continue

            live = live_cols[name]
            expected_type = col_spec["data_type"].lower()
            if live["data_type"] != expected_type:
                violations.append(
                    f"{table}.{name}: type mismatch — contract={expected_type}, live={live['data_type']}"
                )

            expected_null = "YES" if col_spec["nullable"] else "NO"
            if live["is_nullable"] != expected_null:
                violations.append(
                    f"{table}.{name}: nullability mismatch — contract={expected_null}, live={live['is_nullable']}"
                )

            expected_key = col_spec.get("column_key", "")
            # Only enforce PRI/UNI from contract; MUL varies with source indexes.
            if expected_key in ("PRI", "UNI") and live["column_key"] != expected_key:
                violations.append(
                    f"{table}.{name}: key mismatch — contract={expected_key}, live={live['column_key']}"
                )

    live_fingerprint = fingerprint_from_mysql(info_schema_columns)
    expected_fingerprint = load_expected_fingerprint()
    violations.extend(compare_fingerprints(expected_fingerprint, live_fingerprint))

    return violations


def compare_fingerprints(
    expected: dict[str, str], live: dict[str, str]
) -> list[str]:
    violations: list[str] = []
    for table in sorted(set(expected) | set(live)):
        if table not in expected:
            violations.append(f"{table}: unexpected table in live source")
        elif table not in live:
            violations.append(f"{table}: table removed from live source")
        elif expected[table] != live[table]:
            violations.append(
                f"{table}: incompatible schema change (live source does not match contract)"
            )
    return violations


def write_failure_artifact(messages: list[str]) -> None:
    FAILED_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    body = (
        "# Schema validation failed — ingestion stopped\n\n"
        "Live source schema does not match the source-team contract.\n\n"
        + "\n".join(f"- {m}" for m in messages)
    )
    FAILED_SCHEMA_PATH.write_text(body, encoding="utf-8")


def validate_schema(
    *,
    info_schema_columns: list[dict[str, Any]] | None = None,
    current_fingerprint: dict[str, str] | None = None,
    actual_by_table: dict[str, set[str]] | None = None,
) -> list[str]:
    """
    Full validation before ingestion.

    MySQL path: pass info_schema_columns (validates against source contract every time).
    DuckDB/test path: pass current_fingerprint + actual_by_table.
    """
    if info_schema_columns is not None:
        return validate_live_against_source_contract(info_schema_columns)

    if current_fingerprint is None or actual_by_table is None:
        raise ValueError(
            "Provide info_schema_columns for MySQL, or current_fingerprint + actual_by_table"
        )

    violations = check_contract_columns(actual_by_table)
    expected = load_expected_fingerprint()
    violations.extend(compare_fingerprints(expected, current_fingerprint))
    return violations


def save_expected_fingerprint(fingerprint: dict[str, str]) -> None:
    CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_FINGERPRINT_PATH.write_text(
        json.dumps(fingerprint, indent=2, sort_keys=True), encoding="utf-8"
    )


def export_contract_file() -> None:
    """Sync column-list contract from spec (for catalog / legacy checks)."""
    spec = load_source_contract_spec()
    column_list = {
        table: [c["name"] for c in table_def["columns"]]
        for table, table_def in spec["tables"].items()
    }
    CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_CONTRACT_PATH.write_text(
        json.dumps(column_list, indent=2, sort_keys=True), encoding="utf-8"
    )
