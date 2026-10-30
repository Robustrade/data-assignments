"""End-to-end story, reproducibility of the demo, and the CI gate scripts (E2E-01, OPS-06)."""

import subprocess
import sys
from pathlib import Path

import pytest
from demo import Workbench, run_demo, run_story
from pipeline.clock import FakeClock

HARI = Path(__file__).resolve().parent.parent
REPO = HARI.parent.parent


def test_e2e_01_wallet_card_upi_order_to_refund(platform):
    """E2E-01: top-up -> order 2 products -> failed UPI, wallet + card -> ship -> deliver -> refunds."""
    lines = []
    marks = run_story(platform, emit=lines.append)
    assert len(marks) == 7
    # the lake holds every event, exactly once, in order
    source_events = platform.src.execute(
        "SELECT seq FROM change_log ORDER BY seq"
    ).fetchall()
    assert [
        r[0]
        for r in platform.conn.execute(
            "SELECT seq FROM lake.cdc_events ORDER BY seq"
        ).fetchall()
    ] == [r[0] for r in source_events]
    # the warehouse equals the source for every table
    assert platform.warehouse() == platform.snapshot_source()
    # as_of at each step matches the source at that step
    for label, moment, snapshot in marks:
        assert platform.pipeline.as_of(moment) == snapshot, label
    # the story really happened
    order = platform.view("orders")[0]
    assert order["status"] == "partially_refunded" and str(order["total"]) == "330.00"
    payments = platform.conn.execute(
        "SELECT method, status, amount FROM warehouse.payments ORDER BY payment_id"
    ).fetchall()
    assert [(m, s) for m, s, _ in payments] == [
        ("upi", "failed"),
        ("wallet", "success"),
        ("card", "success"),
    ]
    refunds = platform.conn.execute(
        "SELECT r.status, p.method, r.amount, r.wallet_txn_id IS NOT NULL, r.provider_ref IS NOT NULL "
        "FROM warehouse.refunds r JOIN warehouse.payments p USING (payment_id) ORDER BY refund_id"
    ).fetchall()
    assert [(s, m) for s, m, *_ in refunds] == [
        ("processed", "wallet"),
        ("processed", "card"),
    ]
    assert refunds[0][3] and not refunds[0][4] and refunds[1][4] and not refunds[1][3]
    ledger = platform.conn.execute(
        "SELECT type, amount FROM warehouse.wallet_transactions WHERE wallet_id = 'W-0001' ORDER BY wallet_txn_id"
    ).fetchall()
    assert [t for t, _ in ledger] == [
        "topup",
        "topup",
        "payment_debit",
        "refund_credit",
    ]
    # every quality check passed, nothing quarantined, no warnings
    assert (
        platform.scalar(
            "SELECT COUNT(*) FROM quality.check_results WHERE status <> 'pass'"
        )
        == 0
    )
    assert platform.scalar("SELECT COUNT(*) FROM quality.quarantine") == 0
    assert platform.pipeline.verify()["blocking"] == []
    assert platform.pipeline.status()["status"] == "RUNNING"
    # reserved stock moved as the inventory flow says: 2 toasters + 1 kettle shipped
    stock = {
        r[0]: (r[1], r[2])
        for r in platform.conn.execute(
            "SELECT product_id, on_hand, reserved FROM warehouse.inventory"
        ).fetchall()
    }
    assert stock == {"P-0001": (9, 0), "P-0002": (3, 0), "P-0003": (3, 0)}
    assert any("partially_refunded" in line for line in lines)


def lake_and_warehouse(wb):
    lake = wb.conn.execute("SELECT * FROM lake.cdc_events ORDER BY seq").fetchall()
    tables = {}
    for table in wb.contract.tables:
        tables[table] = wb.conn.execute(
            f"SELECT * FROM warehouse.{table}_history ORDER BY seq"
        ).fetchall()
        tables[table + "_current"] = wb.conn.execute(
            f"SELECT * FROM warehouse.{table}_current ORDER BY ALL"
        ).fetchall()
    return lake, tables


def test_ops_06_demo_workload_is_reproducible(tmp_path):
    """OPS-06: running the demo workload twice from scratch gives identical output, lake and warehouse."""
    out_a, out_b = [], []
    first = run_demo(tmp_path / "a", out_a.append)
    second = run_demo(tmp_path / "b", out_b.append)
    assert out_a == out_b and len(out_a) > 50
    assert lake_and_warehouse(first) == lake_and_warehouse(second)
    assert first.warehouse() == second.warehouse()
    first.close()
    second.close()


def test_demo_script_runs_twice_with_identical_output():
    """`python demo.py` is deterministic when run as a program, from the repo root."""
    runs = [
        subprocess.run(
            [sys.executable, "submission/hari/demo.py"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )
        for _ in range(2)
    ]
    for run in runs:
        assert run.returncode == 0, run.stderr
    assert runs[0].stdout == runs[1].stdout
    assert "STOPPED" in runs[0].stdout and "S14  pass" in runs[0].stdout
    assert runs[0].stderr == ""


@pytest.mark.parametrize(
    ("script", "args", "code", "text"),
    [
        ("check_schema_contracts.py", [], 0, "pipeline STOPPED"),
        ("check_schema_contracts.py", ["--drift"], 1, "does not match the contract"),
        ("run_data_quality_checks.py", [], 0, "S1-S14 clean"),
        (
            "run_data_quality_checks.py",
            ["--inject-violation"],
            1,
            "blocking quality check failed",
        ),
    ],
)
def test_ci_scripts_exit_codes(script, args, code, text):
    """The CI gates exit 0 when healthy and 1 when forced to fail."""
    run = subprocess.run(
        [sys.executable, f"submission/hari/scripts/{script}", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == code, run.stdout + run.stderr
    assert text in run.stdout


def test_workbench_uses_independent_fake_clocks(tmp_path):
    """Two demo runs must not share clock state."""
    a, b = Workbench(tmp_path / "a", clock=FakeClock()), Workbench(
        tmp_path / "b", clock=FakeClock()
    )
    a.clock.advance(100)
    assert a.clock.now() != b.clock.now()
    a.close()
    b.close()
