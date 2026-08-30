#!/usr/bin/env python
"""Data quality check (CI job: data-quality-check).

Runs the whole platform from a clean state — seed, full change workload,
ingestion, warehouse build — and then asserts both halves of quality:

* platform-level checks (reconciliation, completeness, durability, freshness,
  validation parity) from cdclake.quality;
* the in-warehouse assertions, by running ``dbt build``.

Failing either fails the job. Nothing is skipped silently: every check is
printed with its result and the number that produced it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake import quality  # noqa: E402
from cdclake.pipeline import (  # noqa: E402
    run_reference_pipeline,
    temporary_workspace,
)


def main() -> int:
    with temporary_workspace() as tmp:
        workspace_root = tmp / "workspace"
        print("data quality check")
        print(f"  workspace: {workspace_root}\n")

        platform = run_reference_pipeline(workspace_root, build=False)
        print(f"  checkpoint      : {platform.lake.checkpoint()}")
        print(f"  batches         : {len(platform.lake.batches())}")
        print(f"  change records  : {sum(1 for _ in platform.lake.records())}")

        print("\nrunning dbt build (models + 127 in-warehouse assertions)")
        dbt_outcome = platform.warehouse.build(fail_fast=False)
        print(f"  dbt build: {'PASS' if dbt_outcome.success else 'FAIL'}")

        results = quality.run_platform_checks(platform)
        report, platform_ok = quality.summarise(results)
        print(report)

        platform.close()

    failures = [r for r in results if not r.passed]
    print(
        f"\n{len(results) - len(failures)}/{len(results)} platform checks passed; "
        f"dbt build {'passed' if dbt_outcome.success else 'FAILED'}"
    )
    ok = platform_ok and dbt_outcome.success
    print("result:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
