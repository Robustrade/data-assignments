"""
Tests — end-to-end pipeline scenarios.

These tests exercise the full happy / unhappy paths through:
  source schema → CDC capture → lake → warehouse → quality checks → catalog

Scenarios:
  1. Cold start: source + bootstrap + seed → all layers populated and consistent
  2. Hot loop:   incremental change → re-run tick → only deltas applied
  3. Restart:   second tick from same checkpoint is a no-op
  4. Stop-the-line: schema break aborts before any data is moved
  5. Recovery:  fixing the schema lets the pipeline resume from where it left off
  6. Restore: rolling current state back to T does not corrupt history or DQ
  7. Quality parity: warehouse rules pass on the seeded state
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pipeline.cdc import CDCCapture, SchemaContractViolation
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.warehouse import (
    apply_cdc_records,
    restore_current_state_to,
    state_at,
)
from scripts.run_data_quality_checks import run_checks
from source.seed import build_seed_rows


def _ts(seconds: int = 0) -> datetime:
    # Far enough in the future that we are always after the seed's
    # captured_at (which uses datetime.now() at test-run time).
    base = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds)


# ── 1. cold start ────────────────────────────────────────────────────────────


def test_cold_start_populates_all_layers(seeded):
    conn, capture = seeded
    n_lake = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    n_wh = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    n_hist = conn.execute(
        "SELECT COUNT(*) FROM wh_customers_history WHERE _is_current"
    ).fetchone()[0]

    assert n_lake == capture.latest_sequence
    assert n_wh == 3
    assert n_hist == 3


def test_cold_start_passes_all_quality_rules(seeded):
    conn, _ = seeded
    failures = run_checks(conn)
    assert failures == [], (
        "post-seed warehouse should be clean, got: "
        + ", ".join(f"{r.rule_id}: {n}" for r, n in failures)
    )


# ── 2. hot loop ──────────────────────────────────────────────────────────────


def test_incremental_update_only_applies_delta(seeded, detector):
    conn, capture = seeded
    cp_before = conn.execute(
        "SELECT sequence FROM pipeline_checkpoints WHERE stage = 'warehouse'"
    ).fetchone()[0]

    capture.update(
        "wallets", "w_alice_usd",
        before={"wallet_id": "w_alice_usd", "balance": Decimal("75.0000")},
        after={
            "wallet_id":   "w_alice_usd",
            "customer_id": "c_alice",
            "currency":    "USD",
            "balance":     Decimal("75.0000"),
            "status":      "frozen",                 # status change
            "created_at":  datetime(2026, 6, 1, 9, tzinfo=timezone.utc),
            "updated_at":  _ts(),
        },
        captured_at=_ts(),
    )

    stats = run_tick(conn, capture, detector=detector)
    assert stats["lake_written"] == 1
    assert stats["warehouse_applied"] == 1
    assert stats["checkpoint_warehouse"] > cp_before


# ── 3. restart / no-op ───────────────────────────────────────────────────────


def test_second_tick_with_no_new_events_is_noop(seeded, detector):
    conn, capture = seeded
    stats = run_tick(conn, capture, detector=detector)
    assert stats["lake_written"] == 0
    assert stats["warehouse_applied"] == 0


def test_restart_resumes_from_checkpoint(detector_factory_fresh_conn):
    """
    Simulate a process restart:
      tick 1 -> seed half the rows
      tick 2 -> add the rest, but with a "fresh" detector instance
    The pipeline should pick up exactly where checkpoint stopped.
    """
    conn, detector = detector_factory_fresh_conn
    bootstrap(conn)
    capture = CDCCapture()
    rows = build_seed_rows()
    half = len(rows) // 2

    for r in rows[:half]:
        capture.insert(r.table, r.pk, r.data)
    s1 = run_tick(conn, capture, detector=detector)
    assert s1["lake_written"] == half

    for r in rows[half:]:
        capture.insert(r.table, r.pk, r.data)
    s2 = run_tick(conn, capture, detector=detector)
    assert s2["lake_written"] == len(rows) - half
    assert s2["checkpoint_warehouse"] == capture.latest_sequence


# ── 4. stop-the-line ─────────────────────────────────────────────────────────


def test_breaking_drift_blocks_lake_and_warehouse(seeded, detector):
    """
    Even if there is a fresh CDC event waiting, a breaking drift on the
    source must abort BEFORE any data is moved.
    """
    conn, capture = seeded
    n_lake_before = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]

    # Make capture think a customer changed.
    capture.update(
        "customers", "c_alice",
        before={"customer_id": "c_alice", "status": "active"},
        after={"customer_id": "c_alice", "status": "suspended"},
        captured_at=_ts(),
    )

    # Now corrupt the source schema (drop a contracted column).
    for t in ("ledger_entries", "transfer_events", "transfers",
              "payment_methods", "wallets"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    stats = run_tick(conn, capture, detector=detector)
    assert stats["aborted"]
    assert stats["reason"] == "schema_contract_violation"

    n_lake_after = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    # Nothing was written.
    assert n_lake_before == n_lake_after


def test_strict_capture_refuses_writes_when_drift_breaking(seeded, detector):
    conn, _ = seeded
    for t in ("ledger_entries", "transfer_events", "transfers",
              "payment_methods", "wallets"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    strict = CDCCapture(detector, require_compatible_schema=True)
    with pytest.raises(SchemaContractViolation):
        strict.insert("customers", "c_x", {"customer_id": "c_x"})


# ── 5. recovery from breaking drift ──────────────────────────────────────────


def test_pipeline_resumes_after_schema_repaired(seeded, detector):
    """
    Schema break -> pipeline aborts -> source is repaired -> next tick
    successfully drains queued events.
    """
    from source.models import create_source_tables, drop_source_tables

    conn, capture = seeded

    capture.update(
        "customers", "c_alice",
        before={"customer_id": "c_alice", "status": "active"},
        after={"customer_id": "c_alice", "status": "suspended"},
        captured_at=_ts(),
    )

    # Break source.
    for t in ("ledger_entries", "transfer_events", "transfers",
              "payment_methods", "wallets"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    s_aborted = run_tick(conn, capture, detector=detector)
    assert s_aborted["aborted"]

    # Repair: rebuild source schema to match contracts.
    drop_source_tables(conn)
    create_source_tables(conn)
    s_resumed = run_tick(conn, capture, detector=detector)
    assert not s_resumed["aborted"]
    assert s_resumed["warehouse_applied"] >= 1


# ── 6. restore (history preserved) ───────────────────────────────────────────


def test_restore_does_not_break_dq_rules(seeded):
    """
    After restoring wallets to their seeded state, all DQ rules must
    still pass. This catches restore bugs that would, e.g., leave
    a stale _cdc_seq blocking future merges or violate ledger
    reconciliation.
    """
    conn, capture = seeded

    # Push a benign update on alice's wallet.
    capture.update(
        "wallets", "w_alice_usd",
        before={"wallet_id": "w_alice_usd", "balance": Decimal("75.0000")},
        after={
            "wallet_id":   "w_alice_usd",
            "customer_id": "c_alice",
            "currency":    "USD",
            "balance":     Decimal("75.0000"),     # unchanged
            "status":      "frozen",
            "created_at":  datetime(2026, 6, 1, 9, tzinfo=timezone.utc),
            "updated_at":  _ts(),
        },
        captured_at=_ts(),
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    # Roll wh_wallets back to BEFORE the update.
    restore_current_state_to(conn, "wallets", _ts(-1))

    # Sanity: all wallets present, balance == seed
    bal = conn.execute(
        "SELECT balance FROM wh_wallets WHERE wallet_id = 'w_alice_usd'"
    ).fetchone()[0]
    assert bal == Decimal("75.0000")

    # All quality rules must still pass.
    failures = run_checks(conn)
    assert failures == [], (
        "restore should not corrupt DQ; got: "
        + ", ".join(f"{r.rule_id}: {n}" for r, n in failures)
    )


# ── 7. delete + reinsert lifecycle ───────────────────────────────────────────


def test_delete_then_reinsert_same_pk_is_handled(seeded):
    conn, capture = seeded
    capture.delete(
        "transfers", "tx_failed_003",
        before={"transfer_id": "tx_failed_003"},
        captured_at=_ts(0),
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    # Reinsert with the same PK after the delete (rare but possible).
    capture.insert(
        "transfers", "tx_failed_003", {
            "transfer_id":       "tx_failed_003",
            "from_wallet_id":    "w_chen_usd",
            "to_wallet_id":      "w_bob_gbp",
            "payment_method_id": "pm_chen_upi",
            "amount":            Decimal("5.0000"),
            "currency":          "USD",
            "status":            "pending",
            "reference":         "groceries-retry",
            "failure_reason":    None,
            "created_at":        _ts(60),
            "settled_at":        None,
        },
        captured_at=_ts(60),
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    row = conn.execute(
        "SELECT status, _deleted FROM wh_transfers WHERE transfer_id = 'tx_failed_003'"
    ).fetchone()
    assert row == ("pending", False)


# ── fixtures local to this module ────────────────────────────────────────────


@pytest.fixture()
def detector_factory_fresh_conn(conn, detector):
    return conn, detector
