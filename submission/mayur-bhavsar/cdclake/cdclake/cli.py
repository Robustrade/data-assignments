"""Operator entry point.

    python -m cdclake demo                 full end-to-end run in .work/demo
    python -m cdclake ingest               drain the source into the lake
    python -m cdclake build                rebuild + test the warehouse
    python -m cdclake status               checkpoint, backlog, pipeline state
    python -m cdclake restore --entity customer_order --as-of <ts>
    python -m cdclake acknowledge --operator you --note "..."
    python -m cdclake republish-contracts  adopt the live schema (break glass)
    python -m cdclake fixture              refresh the committed lake extract

Every command that mutates state prints what it did; nothing is silent.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from cdclake import contracts as contracts_mod
from cdclake import source as source_mod
from cdclake.config import DEFAULT_WORKSPACE, FIXTURE_LAKE_FILE, Workspace
from cdclake.gate import (
    IngestionHalted,
    PipelineState,
    acknowledge,
    republish_contracts,
)
from cdclake.lake import Lake
from cdclake.pipeline import build_platform, reconcile
from cdclake.warehouse import Warehouse

log = logging.getLogger("cdclake")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)-7s %(name)s | %(message)s",
    )


# ── commands ────────────────────────────────────────────────────────────────
def cmd_demo(args: argparse.Namespace) -> int:
    """Seed, ingest, apply the full workload, ingest again, build, reconcile."""
    platform = build_platform(args.workspace, fresh=True, seed=True)
    print(f"workspace           : {platform.workspace.root}")
    print(f"seed events ingested: {platform.ingest()}")

    for name, phase in source_mod.WORKLOAD_PHASES:
        phase(platform.conn)
        ingested = platform.ingest()
        print(f"  phase {name:<18} -> {ingested:>3} events")

    print(f"checkpoint          : {platform.lake.checkpoint()}")
    print(f"batches committed   : {len(platform.lake.batches())}")
    platform.warehouse.build()

    print("\nreconciliation (source vs warehouse current state)")
    for table, counts in reconcile(platform).items():
        flag = "OK " if counts["delta"] == 0 else "BAD"
        print(
            f"  {flag} {table:<16} source={counts['source']:<4} "
            f"warehouse={counts['warehouse']:<4} delta={counts['delta']}"
        )
    platform.close()
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    platform = build_platform(args.workspace)
    try:
        results = platform.runner.run_until_caught_up()
    except IngestionHalted as exc:
        print(f"HALTED: {exc}", file=sys.stderr)
        return 2
    total = sum(r.batch.event_count for r in results)
    print(
        f"{total} events in {len(results)} batch(es); "
        f"checkpoint now {platform.lake.checkpoint()}"
    )
    platform.close()
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    warehouse = Warehouse(Workspace.create(args.workspace))
    outcome = warehouse.build(fail_fast=False)
    print("dbt build:", "OK" if outcome.success else f"FAILED — {outcome.message}")
    return 0 if outcome.success else 1


def cmd_status(args: argparse.Namespace) -> int:
    platform = build_platform(args.workspace)
    state = PipelineState.load(platform.workspace)
    print(f"state       : {state.state}")
    if state.is_halted:
        print(f"reason      : {state.reason}")
        print(f"detected_at : {state.detected_at}")
        for delta in state.contract_diff:
            print(
                f"  - [{delta['severity']}] {delta['kind']} {delta.get('column') or ''}"
            )
    print(f"checkpoint  : {platform.lake.checkpoint()}")
    print(f"backlog     : {platform.runner.backlog()} event(s)")
    print(f"batches     : {len(platform.lake.batches())}")
    orphans = platform.lake.orphan_files()
    print(f"orphan files: {len(orphans)}")
    platform.close()
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    warehouse = Warehouse(Workspace.create(args.workspace))
    if args.at_version is not None:
        rows = warehouse.as_of_version(args.entity, args.at_version)
        at = f"version {args.at_version}"
    else:
        rows = warehouse.as_of(args.entity, args.as_of)
        at = args.as_of
    print(json.dumps(rows, indent=2, default=str))
    print(f"\n{len(rows)} row(s) live in {args.entity} at {at}", file=sys.stderr)
    return 0


def cmd_acknowledge(args: argparse.Namespace) -> int:
    workspace = Workspace.create(args.workspace)
    state = acknowledge(workspace, operator=args.operator, note=args.note)
    print(f"state is now {state.state} (acknowledged by {state.acknowledged_by})")
    return 0


def cmd_republish(args: argparse.Namespace) -> int:
    platform = build_platform(args.workspace)
    written = republish_contracts(platform.conn)
    for path in written:
        contract = contracts_mod.TableContract.from_dict(
            json.loads(path.read_text(encoding="utf-8"))
        )
        print(f"{path.name}: contract_version -> {contract.contract_version}")
    print(
        "\nNow regenerate the warehouse spec: "
        "python scripts/generate_dbt_entities.py"
    )
    platform.close()
    return 0


def cmd_fixture(args: argparse.Namespace) -> int:
    """Refresh the committed lake extract used by the dbt-only CI job."""
    platform = build_platform(args.workspace, fresh=True, seed=True)
    platform.ingest()
    source_mod.run_workload(platform.conn)
    platform.ingest()
    lake = Lake(platform.workspace)
    FIXTURE_LAKE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with FIXTURE_LAKE_FILE.open("w", encoding="utf-8") as handle:
        for record in lake.records():
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    count = sum(1 for _ in lake.records())
    print(f"wrote {FIXTURE_LAKE_FILE} ({count} change records)")
    platform.close()
    return 0


# ── argument parsing ────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cdclake", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=DEFAULT_WORKSPACE,
        help="root directory for source db, lake, warehouse and state",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("demo", help="full end-to-end run from a clean state").set_defaults(
        func=cmd_demo
    )
    sub.add_parser("ingest", help="drain the source into the lake").set_defaults(
        func=cmd_ingest
    )
    sub.add_parser("build", help="rebuild and test the warehouse").set_defaults(
        func=cmd_build
    )
    sub.add_parser(
        "status", help="checkpoint, backlog and pipeline state"
    ).set_defaults(func=cmd_status)
    sub.add_parser(
        "republish-contracts",
        help="adopt the live source schema as the new contract (break glass)",
    ).set_defaults(func=cmd_republish)
    sub.add_parser(
        "fixture", help="refresh the committed lake extract for CI"
    ).set_defaults(func=cmd_fixture)

    restore = sub.add_parser(
        "restore", help="reconstruct an entity at a timestamp or a change version"
    )
    restore.add_argument("--entity", required=True)
    group = restore.add_mutually_exclusive_group(required=True)
    group.add_argument("--as-of", help="UTC timestamp, e.g. 2026-08-29T12:00:00.000Z")
    group.add_argument(
        "--at-version",
        type=int,
        help="change version (event id). Exact; prefer this for a real restore",
    )
    restore.set_defaults(func=cmd_restore)

    ack = sub.add_parser("acknowledge", help="clear a HALTED pipeline")
    ack.add_argument("--operator", required=True)
    ack.add_argument("--note", required=True, help="why it is safe to resume")
    ack.set_defaults(func=cmd_acknowledge)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(args.verbose)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
