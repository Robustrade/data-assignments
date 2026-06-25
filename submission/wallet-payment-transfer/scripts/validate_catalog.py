#!/usr/bin/env python3
"""
validate_catalog.py

CI entry point for the catalog metadata check (`catalog-check`).

What it verifies
----------------
  - catalog/catalog.json exists and parses as JSON
  - it lists exactly one lake dataset (`lake_cdc_events`)
  - it lists every expected `wh_<table>` current-state dataset
  - it lists every expected `wh_<table>_history` SCD2 dataset
  - every entry has the required metadata fields
  - layer labels are consistent with naming conventions
"""

from __future__ import annotations

import json
import os
import sys

CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "catalog",
    "catalog.json",
)


# Same source tables used by the warehouse layer; redeclared here to keep
# the script standalone (no DuckDB import cost on a metadata check).
SOURCE_TABLES = (
    "customers",
    "payment_methods",
    "wallets",
    "transfers",
    "transfer_events",
    "ledger_entries",
)

REQUIRED_FIELDS = (
    "name",
    "layer",
    "type",
    "description",
    "owner",
    "consumers",
    "update_cadence",
    "tags",
)


def expected_dataset_names() -> list[str]:
    names = ["lake_cdc_events"]
    names.extend(f"wh_{t}" for t in SOURCE_TABLES)
    names.extend(f"wh_{t}_history" for t in SOURCE_TABLES)
    return names


def validate() -> list[str]:
    issues: list[str] = []
    if not os.path.exists(CATALOG_PATH):
        return [f"catalog.json not found at {CATALOG_PATH}"]

    with open(CATALOG_PATH) as f:
        try:
            catalog = json.load(f)
        except json.JSONDecodeError as exc:
            return [f"catalog.json is not valid JSON: {exc}"]

    if "datasets" not in catalog or not isinstance(catalog["datasets"], list):
        return ["catalog.json missing top-level 'datasets' list"]

    by_name = {d.get("name"): d for d in catalog["datasets"] if "name" in d}

    for name in expected_dataset_names():
        if name not in by_name:
            issues.append(f"missing dataset entry: {name}")
            continue
        entry = by_name[name]
        for field in REQUIRED_FIELDS:
            if field not in entry or not entry[field]:
                issues.append(
                    f"{name}: missing or empty required field '{field}'"
                )

    # layer label consistency
    if "lake_cdc_events" in by_name:
        if by_name["lake_cdc_events"].get("layer") != "lake":
            issues.append("lake_cdc_events: layer must be 'lake'")
    for name, entry in by_name.items():
        if name.startswith("wh_") and entry.get("layer") != "warehouse":
            issues.append(f"{name}: layer must be 'warehouse'")
        if name.endswith("_history") and entry.get("type") != "scd2-history":
            issues.append(f"{name}: type must be 'scd2-history'")

    return issues


def main() -> int:
    issues = validate()
    if issues:
        print("Catalog validation failures:")
        for v in issues:
            print(f"  x {v}")
        return 1
    n = len(expected_dataset_names())
    print(f"Catalog validation passed ({n} datasets verified).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
