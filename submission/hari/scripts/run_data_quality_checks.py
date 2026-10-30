"""Validation-parity gate: run the E2E workload and require every S1–S14 check to pass.

Usage: python submission/hari/scripts/run_data_quality_checks.py [--inject-violation]
``--inject-violation`` adds a corrupt event after the workload (negative test: exit 1).
"""

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from demo import Workbench, run_story
from pipeline.cdc.reader import Event
from pipeline.errors import QualityBlocked


def main(argv: list[str]) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        wb = Workbench(tmp)
        try:
            run_story(wb, emit=lambda *_: None)
            if "--inject-violation" in argv:
                row = dict(
                    wb.src.execute("SELECT * FROM order_items LIMIT 1").fetchone()
                )
                bad = Event(
                    seq=10_000,
                    txn_id="X-1",
                    schema_version=1,
                    table_name="order_items",
                    op="update",
                    pk={"order_id": row["order_id"], "line_no": row["line_no"]},
                    before=row,
                    after={**row, "line_total_paise": 1},
                    changed_at=wb.clock.now(),
                )
                try:
                    wb.pipeline.process_events([bad])
                except QualityBlocked as err:
                    print(f"FAIL: {err}")
                    return 1
            results = wb.conn.execute(
                "SELECT rule_id, count(*) AS batches, sum(violations) AS violations, "
                "count(*) FILTER (WHERE status = 'fail') AS failed FROM quality.check_results "
                "GROUP BY rule_id ORDER BY CAST(substr(rule_id, 2) AS INTEGER)"
            ).fetchall()
            report = wb.pipeline.verify()
            problems = 0
            for rule, batches, violations, failed in results:
                state = "pass" if failed == 0 else "FAIL"
                problems += failed
                print(
                    f"{rule:<4} {state}  ({batches} batches, {violations} violations)"
                )
            status = wb.pipeline.status()["status"]
            quarantined = wb.scalar("SELECT COUNT(*) FROM quality.quarantine")
            print(
                f"full-warehouse verification: {len(report['blocking'])} blocking, {len(report['quarantined'])} quarantined"
            )
            print(f"pipeline status: {status}; quarantined rows: {quarantined}")
            if (
                len(results) != 14
                or problems
                or report["blocking"]
                or report["quarantined"]
                or status != "RUNNING"
            ):
                print("data quality check FAILED")
                return 1
            print("data quality check passed: S1-S14 clean")
            return 0
        finally:
            wb.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
