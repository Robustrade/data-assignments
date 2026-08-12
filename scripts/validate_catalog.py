#!/usr/bin/env python3
"""
validate_catalog.py

Validates the completeness, structure, and accuracy of catalog metadata declared in
catalog/catalog.json. Ensures all lake and warehouse datasets publish required owner,
description, layer, and field schema information.

Exit 0 — catalog metadata valid.
Exit 1 — metadata validation failure.
"""

from __future__ import annotations

import json
import os
import sys

# Find catalog file location
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(script_dir)
catalog_path = os.path.join(root_dir, "catalog", "catalog.json")

REQUIRED_DATASET_FIELDS = [
    "name",
    "layer",
    "table_name",
    "owner",
    "intended_audience",
    "update_cadence",
    "description",
    "schema",
]

REQUIRED_SCHEMA_FIELDS = ["name", "type", "description", "nullable"]
ALLOWED_LAYERS = {"lake", "warehouse"}


def validate_catalog_file(path: str) -> list[str]:
    """Validate catalog JSON file against required structural metadata contract."""
    errors: list[str] = []

    if not os.path.exists(path):
        return [f"Catalog file not found at path: {path}"]

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        return [f"Failed to parse JSON catalog file: {exc}"]

    if "version" not in data:
        errors.append("Catalog missing root field: 'version'")
    if "datasets" not in data or not isinstance(data["datasets"], list):
        errors.append("Catalog missing root list field: 'datasets'")
        return errors

    datasets = data["datasets"]
    if len(datasets) == 0:
        errors.append("Catalog 'datasets' list is empty.")

    lake_count = 0
    warehouse_count = 0

    for idx, ds in enumerate(datasets):
        ds_name = ds.get("name", f"dataset[{idx}]")

        for fld in REQUIRED_DATASET_FIELDS:
            if fld not in ds or ds[fld] is None or ds[fld] == "":
                errors.append(f"Dataset '{ds_name}' missing required field: '{fld}'")

        layer = ds.get("layer")
        if layer not in ALLOWED_LAYERS:
            errors.append(
                f"Dataset '{ds_name}' has invalid layer '{layer}'. Must be one of: {sorted(ALLOWED_LAYERS)}"
            )
        elif layer == "lake":
            lake_count += 1
        elif layer == "warehouse":
            warehouse_count += 1

        schema_cols = ds.get("schema")
        if isinstance(schema_cols, list):
            if len(schema_cols) == 0:
                errors.append(f"Dataset '{ds_name}' schema column list is empty.")
            for col_idx, col in enumerate(schema_cols):
                col_name = col.get("name", f"col[{col_idx}]")
                for sfld in REQUIRED_SCHEMA_FIELDS:
                    if sfld not in col:
                        errors.append(
                            f"Dataset '{ds_name}' column '{col_name}' missing required attribute: '{sfld}'"
                        )
        else:
            errors.append(
                f"Dataset '{ds_name}' field 'schema' must be a list of columns."
            )

    if lake_count == 0:
        errors.append("Catalog contains no 'lake' datasets.")
    if warehouse_count == 0:
        errors.append("Catalog contains no 'warehouse' datasets.")

    return errors


def main() -> int:
    errors = validate_catalog_file(catalog_path)

    if errors:
        print("Catalog metadata validation FAILED:")
        for err in errors:
            print(f"  ✗ {err}")
        return 1

    print(f"Catalog metadata validation PASSED ({catalog_path}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
