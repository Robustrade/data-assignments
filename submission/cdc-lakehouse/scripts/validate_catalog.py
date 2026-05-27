#!/usr/bin/env python3
"""Validate catalog metadata file."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "catalog" / "catalog.json"


def main() -> int:
    if not CATALOG_PATH.exists():
        print(f"Missing catalog file: {CATALOG_PATH}")
        return 1

    data = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    datasets = data.get("datasets", [])
    if not datasets:
        print("Catalog has no datasets.")
        return 1

    layers = {d.get("layer") for d in datasets}
    if "lake" not in layers or "warehouse" not in layers:
        print("Catalog must include both lake and warehouse datasets.")
        return 1

    required = {
        "name",
        "layer",
        "description",
        "owner",
        "consumers",
        "update_cadence",
        "location",
    }
    violations: list[str] = []
    for ds in datasets:
        missing = required - set(ds.keys())
        if missing:
            violations.append(
                f"{ds.get('name', '?')}: missing fields {sorted(missing)}"
            )

    if violations:
        for v in violations:
            print(f"  ✗ {v}")
        return 1

    print(f"Catalog valid ({len(datasets)} datasets).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
