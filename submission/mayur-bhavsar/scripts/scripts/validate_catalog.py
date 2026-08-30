#!/usr/bin/env python
"""Catalog validation (CI job: catalog-check).

Checks the catalog against reality rather than against itself:

* every entry carries the metadata a consumer needs to use the dataset safely
  (owner, intended consumers, cadence, access path, PII flag, lineage);
* the catalogued lake schema equals the physical lake record;
* every catalogued warehouse dataset is a real dbt model, and — the direction
  people forget — every dbt model is catalogued, so nothing ships undocumented;
* every catalogued column set equals the live relation's columns, checked
  against a warehouse built from scratch during this run.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake.catalog import (  # noqa: E402
    Catalog,
    render_report,
    validate_against_warehouse,
    validate_lake_schema,
    validate_models_exist,
    validate_structure,
)
from cdclake.pipeline import (  # noqa: E402
    run_reference_pipeline,
    temporary_workspace,
)


def main() -> int:
    catalog = Catalog.load()
    print("catalog validation")
    print(
        f"  datasets: {len(catalog.datasets)} "
        f"({len(catalog.by_layer('lake'))} lake, "
        f"{len(catalog.by_layer('warehouse'))} warehouse)\n"
    )

    sections = [
        ("required metadata is present on every dataset", validate_structure(catalog)),
        (
            "catalogued lake schema matches the physical record",
            validate_lake_schema(catalog),
        ),
        (
            "catalog and dbt models agree in both directions",
            validate_models_exist(catalog),
        ),
    ]

    with temporary_workspace() as tmp:
        platform = run_reference_pipeline(tmp / "workspace")
        sections.append(
            (
                "catalogued schemas match the live warehouse",
                validate_against_warehouse(catalog, platform.warehouse),
            )
        )
        platform.close()

    report, ok = render_report(sections)
    print(report)
    print("\nresult:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
