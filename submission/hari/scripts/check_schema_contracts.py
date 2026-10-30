"""Schema-contract gate.

1. A freshly built source must match the contract.
2. A breaking change (a dropped column) must make the pipeline STOP.

Usage: python submission/hari/scripts/check_schema_contracts.py [--drift]
``--drift`` applies the breaking change *before* step 1 (negative test of the gate: exit 1).
"""

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from demo import Workbench, seed_basic
from pipeline.contract import diff_live, introspect
from pipeline.errors import SchemaContractViolation


def break_schema(src) -> None:
    for name in ("cdc_users_ins", "cdc_users_upd", "cdc_users_del"):
        src.execute(f"DROP TRIGGER IF EXISTS {name}")
    src.execute("ALTER TABLE users DROP COLUMN phone")


def main(argv: list[str]) -> int:
    drift = "--drift" in argv
    with tempfile.TemporaryDirectory() as tmp:
        wb = Workbench(tmp)
        try:
            ids = seed_basic(wb.app)
            wb.sync()
            if drift:
                break_schema(wb.src)
            violations, _ = diff_live(wb.contract, introspect(wb.src))
            if violations:
                print("FAIL: the source does not match the contract:")
                for v in violations:
                    print(f"  - {v['message']}")
                return 1
            print("ok: source matches contract version", wb.contract.version)
            break_schema(wb.src)
            wb.app.restock(ids["p1"], 1)
            try:
                wb.pipeline.run_once()
            except SchemaContractViolation as err:
                status = wb.pipeline.status()["status"]
                if status != "STOPPED":
                    print(f"FAIL: violation raised but status is {status}")
                    return 1
                print(
                    f"ok: breaking change detected and pipeline STOPPED ({len(err.violations)} violation(s))"
                )
                return 0
            print("FAIL: a dropped column was not detected")
            return 1
        finally:
            wb.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
