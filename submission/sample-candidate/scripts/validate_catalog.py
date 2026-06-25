#!/usr/bin/env python3
"""
validate_catalog.py

Verifies that catalog/catalog.json is present and contains a complete entry
for every required lake and warehouse dataset.

Required fields per entry: name, layer, description, owner, schema, update_cadence

Exit 0 — catalog is valid.
Exit 1 — missing file, missing datasets, or missing required fields.
"""

import json
import os
import sys

CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "catalog",
    "catalog.json",
)

REQUIRED_DATASETS = [
    "lake_cdc_events",
    "wh_customers",
    "wh_wallets",
    "wh_transactions",
]

REQUIRED_FIELDS = ["name", "layer", "description", "owner", "schema", "update_cadence"]


def validate() -> list[str]:
    violations: list[str] = []

    if not os.path.exists(CATALOG_PATH):
        violations.append(f"catalog.json not found at {CATALOG_PATH}")
        return violations

    with open(CATALOG_PATH) as f:
        try:
            catalog = json.load(f)
        except json.JSONDecodeError as exc:
            violations.append(f"catalog.json is not valid JSON: {exc}")
            return violations

    datasets = {d["name"]: d for d in catalog.get("datasets", []) if "name" in d}

    for name in REQUIRED_DATASETS:
        if name not in datasets:
            violations.append(f"Missing dataset entry: {name}")
            continue

        entry = datasets[name]
        for field in REQUIRED_FIELDS:
            if field not in entry or not entry[field]:
                violations.append(f"{name}: missing or empty required field '{field}'")

    return violations


def main() -> int:
    violations = validate()

    if violations:
        print("Catalog validation failures:")
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print(f"Catalog validation passed ({len(REQUIRED_DATASETS)} datasets verified).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
