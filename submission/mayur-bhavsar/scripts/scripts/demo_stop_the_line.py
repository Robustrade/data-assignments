#!/usr/bin/env python
"""Narrated demonstration of the stop-the-line behaviour.

    python scripts/demo_stop_the_line.py [mutation]

Walks the whole incident: a breaking source change is applied, the gate halts
the pipeline before reading a row, a restart is refused, and an operator
performs the break-glass procedure to resume.

Exits non-zero if any step does not behave as documented, so this doubles as an
executable specification of docs/03-schema-evolution.md.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake import source as source_mod  # noqa: E402
from cdclake.gate import (  # noqa: E402
    IngestionHalted,
    PipelineState,
    acknowledge,
    republish_contracts,
)
from cdclake.lake import read_ndjson  # noqa: E402
from cdclake.pipeline import build_platform, temporary_workspace  # noqa: E402


def rule(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def main(argv: list[str]) -> int:
    mutation = argv[1] if len(argv) > 1 else "drop_column"
    if mutation not in source_mod.SCHEMA_MUTATIONS:
        print(f"unknown mutation {mutation!r}", file=sys.stderr)
        print(f"choose one of: {sorted(source_mod.SCHEMA_MUTATIONS)}", file=sys.stderr)
        return 2

    with temporary_workspace() as tmp:
        root = tmp / "workspace"
        platform = build_platform(root, fresh=True, seed=True)

        rule("1. steady state")
        ingested = platform.ingest()
        checkpoint = platform.lake.checkpoint()
        print(f"ingested {ingested} change(s); checkpoint = {checkpoint}")

        rule(f"2. the source team ships a breaking change: {mutation}")
        source_mod.apply_mutation(platform.conn, mutation)
        print("the source schema no longer matches the published contract")

        rule("3. the next batch runs")
        try:
            platform.runner.run_batch()
        except IngestionHalted as exc:
            print(f"ingestion refused to proceed: {exc}")
        else:
            print("FAIL: the pipeline kept ingesting under a broken contract")
            return 1

        state = PipelineState.load(platform.workspace)
        print(f"\npipeline state : {state.state}")
        print(f"detected at    : {state.detected_at}")
        print("what changed   :")
        for delta in state.contract_diff:
            where = (
                f"{delta['table']}.{delta['column']}"
                if delta["column"]
                else delta["table"]
            )
            print(f"  - [{delta['severity']}] {delta['kind']} on {where}")
            print(f"      {delta['detail']}")

        if platform.lake.checkpoint() != checkpoint:
            print("FAIL: the checkpoint moved despite the halt")
            return 1
        if platform.lake.orphan_files():
            print("FAIL: partial files were written despite the halt")
            return 1
        print("\nnothing was written: the gate runs before a single row is read")

        alerts = [
            a
            for a in read_ndjson(platform.workspace.alerts_path)
            if a["event"] == "ingestion_halted"
        ]
        print(f"alerts emitted : {len(alerts)} (severity {alerts[-1]['severity']})")

        rule("4. a scheduled retry, five minutes later")
        platform.close()
        retried = build_platform(root)
        try:
            retried.runner.run_batch()
        except IngestionHalted as exc:
            print(f"still refused: {str(exc).splitlines()[0]}")
        else:
            print("FAIL: a halted pipeline restarted itself")
            return 1

        rule("5. break glass — an operator decides")
        scratch = root / "accepted_contracts"
        scratch.mkdir(parents=True, exist_ok=True)
        from cdclake.config import CONTRACTS_DIR

        for path in CONTRACTS_DIR.glob("*.json"):
            (scratch / path.name).write_text(path.read_text("utf-8"), "utf-8")

        republish_contracts(retried.conn, contracts_dir=scratch)
        customer = json.loads((scratch / "customer.json").read_text("utf-8"))
        print(f"contract republished: customer v{customer['contract_version']}")
        acknowledge(
            retried.workspace,
            operator="mayur",
            note=f"source team confirmed {mutation}; contract republished",
        )
        print("halt acknowledged, with operator and reason recorded in the state file")
        retried.close()

        rule("6. ingestion resumes")
        resumed = build_platform(root, contracts_dir=scratch)
        source_mod.workload_price_change(resumed.conn)
        resumed.ingest()
        print(f"checkpoint advanced to {resumed.lake.checkpoint()}")
        print(f"pipeline state: {PipelineState.load(resumed.workspace).state}")
        resumed.close()

    print(
        "\nresult: PASS — the line stopped, stayed stopped, and resumed on a "
        "human decision"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
