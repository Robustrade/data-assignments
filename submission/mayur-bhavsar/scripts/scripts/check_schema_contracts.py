#!/usr/bin/env python
"""Schema contract check (CI job: schema-contract-check).

Three things are proven here, in order of how badly each would bite:

1. **The contracts describe the source.** A pristine database is built from the
   committed DDL, introspected, and diffed against contracts/*.json. Any
   difference means the published contract is a lie.
2. **The warehouse spec is derived, not copied.** macros/_entities.sql must be
   exactly what the generator produces from the contracts.
3. **The gate actually stops the line.** Every breaking mutation in
   cdclake.source is applied to a throwaway database and must be classified
   BREAKING; every compatible mutation must not be. A gate that has never been
   shown to fire is not a control, it is a comment.

Exit code is non-zero if any of the three fails.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake import source as source_mod  # noqa: E402
from cdclake.config import ROOT, Workspace  # noqa: E402
from cdclake.gate import SchemaGate  # noqa: E402
from cdclake.pipeline import temporary_workspace  # noqa: E402


def _fresh_source(directory: Path, name: str):
    return source_mod.create_source(directory / f"{name}.db")


def check_contracts_match_ddl(workdir: Path) -> list[str]:
    conn = _fresh_source(workdir, "pristine")
    verdict = SchemaGate(Workspace.create(workdir / "ws_pristine")).inspect(conn)
    conn.close()
    if verdict.deltas:
        return [str(delta) for delta in verdict.deltas]
    return []


def check_generated_spec_current() -> list[str]:
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "generate_dbt_entities.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return [(result.stdout + result.stderr).strip()]
    return []


def check_gate_fires(workdir: Path) -> list[str]:
    findings: list[str] = []

    for name in source_mod.BREAKING_MUTATIONS:
        conn = _fresh_source(workdir, f"break_{name}")
        source_mod.apply_mutation(conn, name)
        verdict = SchemaGate(Workspace.create(workdir / f"ws_{name}")).inspect(conn)
        conn.close()
        if not verdict.breaking:
            findings.append(
                f"mutation {name!r} was not classified BREAKING "
                f"(deltas: {verdict.render()})"
            )

    for name in source_mod.COMPATIBLE_MUTATIONS:
        conn = _fresh_source(workdir, f"compat_{name}")
        source_mod.apply_mutation(conn, name)
        verdict = SchemaGate(Workspace.create(workdir / f"ws_{name}")).inspect(conn)
        conn.close()
        if verdict.breaking:
            findings.append(
                f"mutation {name!r} should be compatible but halted the pipeline: "
                + "; ".join(str(d) for d in verdict.breaking)
            )
        if not verdict.warnings:
            findings.append(
                f"mutation {name!r} produced no warning; a compatible schema "
                "change must still be surfaced to an operator"
            )

    return findings


def main() -> int:
    print("schema contract check")
    print(f"  contracts : {len(list((ROOT / 'contracts').glob('*.json')))} published")
    print(f"  mutations : {len(source_mod.SCHEMA_MUTATIONS)} exercised\n")

    with temporary_workspace() as workdir:
        sections = [
            ("contracts match the committed DDL", check_contracts_match_ddl(workdir)),
            ("macros/_entities.sql is current", check_generated_spec_current()),
            ("gate classifies every mutation correctly", check_gate_fires(workdir)),
        ]

    ok = True
    for title, findings in sections:
        if findings:
            ok = False
            print(f"FAIL {title}")
            for finding in findings:
                print(f"       - {finding}")
        else:
            print(f"OK   {title}")

    print("\nresult:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
