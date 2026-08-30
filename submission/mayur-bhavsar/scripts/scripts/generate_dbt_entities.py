#!/usr/bin/env python
"""Generate the dbt entity spec from the published contracts.

The warehouse needs to know, for every tracked table, the column list, the
DuckDB type of each column and the enum domains. That information already
exists in ``contracts/*.json``. Rather than restate it in Jinja — where it
would silently drift — it is generated, committed, and checked.

    python scripts/generate_dbt_entities.py            # write the macro
    python scripts/generate_dbt_entities.py --check     # fail if out of date

``--check`` runs in CI (scripts/check_schema_contracts.py) so a contract change
that is not propagated to the warehouse cannot merge.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdclake import contracts as contracts_mod  # noqa: E402
from cdclake.config import ROOT, TRACKED_TABLES  # noqa: E402

TARGET = ROOT / "macros" / "_entities.sql"

HEADER = """\
{#-
    GENERATED FILE - DO NOT EDIT.

    Source of truth : contracts/*.json
    Regenerate with : python scripts/generate_dbt_entities.py
    Verified by     : python scripts/check_schema_contracts.py (--check mode)

    Holding the entity spec in one place is what lets the warehouse models,
    the not-null assertions and the enum domain assertions all be derived from
    the same published contract instead of a hand-maintained copy of it.
-#}
{% macro cdc_entities() %}
{% do return(
"""

FOOTER = """
) %}
{% endmacro %}
"""


def build_spec() -> dict[str, object]:
    spec: dict[str, object] = {}
    for table in TRACKED_TABLES:
        contract = contracts_mod.load_contract(table)
        spec[table] = {
            "primary_key": list(contract.primary_key),
            "contract_version": contract.contract_version,
            "columns": [
                {
                    "name": col.name,
                    "type": contracts_mod.duckdb_type(col.type),
                    "nullable": col.nullable,
                }
                for col in contract.columns
            ],
            "enums": {col.name: list(col.enum) for col in contract.columns if col.enum},
        }
    return spec


def render() -> str:
    body = json.dumps(build_spec(), indent=4)
    return HEADER + body + FOOTER


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed macro is out of date",
    )
    args = parser.parse_args()

    rendered = render()
    if args.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.exists() else ""
        if current != rendered:
            print(
                f"FAIL {TARGET.relative_to(ROOT)} is out of date with contracts/.\n"
                "     Run: python scripts/generate_dbt_entities.py",
                file=sys.stderr,
            )
            return 1
        print(f"OK   {TARGET.relative_to(ROOT)} matches contracts/")
        return 0

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(rendered, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
