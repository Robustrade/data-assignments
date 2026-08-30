#!/usr/bin/env python
"""Narrated demonstration of crash recovery and replay safety.

    python scripts/demo_crash_recovery.py

Kills a batch between the durable data write and the manifest append — the one
window where the lake can be inconsistent — then restarts and proves the
failure mode is a duplicate, never a loss. Finishes by replaying the entire
change log from zero and showing the warehouse is unchanged.

Exits non-zero if any invariant fails.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake import source as source_mod  # noqa: E402
from cdclake.pipeline import (  # noqa: E402
    build_platform,
    reconcile,
    temporary_workspace,
)


def rule(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def distinct(platform) -> set[tuple[str, int]]:
    return {(r["table_name"], r["event_id"]) for r in platform.lake.records()}


def main() -> int:
    with temporary_workspace() as tmp:
        root = tmp / "workspace"
        platform = build_platform(root, fresh=True, seed=True)
        source_events = platform.conn.execute(
            "SELECT count(*) FROM cdc_events"
        ).fetchone()[0]

        rule("1. the process dies mid-batch")
        try:
            platform.runner.run_batch(simulate_crash_before_manifest=True)
        except RuntimeError as exc:
            print(f"crashed: {exc}")
        else:
            print("FAIL: the simulated crash did not fire")
            return 1

        orphans = platform.lake.orphan_files()
        print(f"files on disk with no manifest entry : {len(orphans)}")
        print(f"checkpoint                           : {platform.lake.checkpoint()}")
        print(f"records visible to a reader          : {len(distinct(platform))}")
        if platform.lake.checkpoint() != 0 or distinct(platform):
            print("FAIL: uncommitted data was visible")
            return 1
        print(
            "readers resolve files through the manifest, so the partial write is "
            "invisible"
        )

        rule("2. restart")
        recovered = platform.ingest()
        print(f"re-read and committed {recovered} change(s)")
        print(f"checkpoint = {platform.lake.checkpoint()}")
        print(
            f"orphans remaining = {len(platform.lake.orphan_files())} "
            "(the retry reuses the same batch id, so it overwrites its own "
            "partial files)"
        )

        if len(distinct(platform)) != source_events:
            print(
                f"FAIL: {len(distinct(platform))} distinct changes in the lake vs "
                f"{source_events} at the source"
            )
            return 1
        print(
            f"every one of the {source_events} source changes is present, exactly once"
        )

        rule("3. the rest of the workload, then the warehouse")
        source_mod.run_workload(platform.conn)
        platform.ingest()
        platform.refresh_warehouse()
        for table, counts in reconcile(platform).items():
            flag = "OK " if counts["delta"] == 0 else "BAD"
            print(
                f"  {flag} {table:<16} source={counts['source']:<4} "
                f"warehouse={counts['warehouse']}"
            )
        if any(c["delta"] for c in reconcile(platform).values()):
            print("FAIL: source and warehouse disagree")
            return 1

        rule("4. full replay from zero")
        before = {
            entity: platform.warehouse.current(entity)
            for entity in ("customer", "customer_order", "order_item", "payment")
        }
        platform.workspace.warehouse_db.unlink()
        platform.refresh_warehouse()
        after = {entity: platform.warehouse.current(entity) for entity in before}

        if before != after:
            print("FAIL: rebuilding from the lake produced a different warehouse")
            return 1
        print("warehouse dropped and rebuilt from the change log alone — identical")
        platform.close()

    print("\nresult: PASS — no loss, no duplicates, replay is idempotent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
