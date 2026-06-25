"""
Tests — point-in-time reconstruction (state_at) and restore.

Covers:
  - state_at returns the row that was current at ts
  - state_at returns the most recent revision *strictly* before next change
  - state_at excludes deleted rows
  - state_at returns nothing before the row's _valid_from
  - restore_current_state_to overwrites current-state with snapshot
  - restore is non-destructive to history (history is preserved)
  - restored state is a valid starting point for the next CDC tick
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pipeline.orchestrator import run_tick
from pipeline.warehouse import (
    apply_cdc_records,
    restore_current_state_to,
    state_at,
)


def _ts(seconds: int = 0) -> datetime:
    # Far enough in the future that we are always after the seed's
    # captured_at (which uses datetime.now() at test-run time).
    base = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds)


def _evolve_alice_balance(seeded, balances: list[Decimal]) -> list[datetime]:
    """
    Push a series of balance updates onto w_alice_usd.

    Returns the list of captured_at timestamps used for each update so
    the test can replay queries at exact boundaries.
    """
    conn, capture = seeded
    timestamps: list[datetime] = []
    prev = Decimal("75.0000")
    for i, bal in enumerate(balances):
        ts = _ts(i * 10)
        capture.update(
            "wallets", "w_alice_usd",
            before={"wallet_id": "w_alice_usd", "balance": prev},
            after={
                "wallet_id":   "w_alice_usd",
                "customer_id": "c_alice",
                "currency":    "USD",
                "balance":     bal,
                "status":      "active",
                "created_at":  datetime(2026, 6, 1, 9, tzinfo=timezone.utc),
                "updated_at":  ts,
            },
            captured_at=ts,
        )
        timestamps.append(ts)
        prev = bal
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - len(balances)))
    return timestamps


# ── state_at: basic reconstruction ───────────────────────────────────────────


def test_state_at_returns_seed_state_after_seed(seeded):
    conn, _ = seeded
    rows = state_at(conn, "wallets", _ts(99999))
    by_id = {r["wallet_id"]: r["balance"] for r in rows}
    assert by_id["w_alice_usd"] == Decimal("75.0000")
    assert by_id["w_bob_gbp"] == Decimal("50.0000")


def test_state_at_picks_revision_active_at_timestamp(seeded):
    conn, _ = seeded
    timestamps = _evolve_alice_balance(
        seeded, [Decimal("60.0000"), Decimal("40.0000"), Decimal("20.0000")]
    )

    # Just before the first new revision -> still 75 (the seed value)
    rows = state_at(conn, "wallets", timestamps[0] - timedelta(seconds=1))
    by_id = {r["wallet_id"]: r["balance"] for r in rows}
    assert by_id["w_alice_usd"] == Decimal("75.0000")

    # At the first new revision exactly -> 60
    rows = state_at(conn, "wallets", timestamps[0])
    assert {r["wallet_id"]: r["balance"] for r in rows}["w_alice_usd"] == \
        Decimal("60.0000")

    # Between revisions -> middle value (40)
    rows = state_at(conn, "wallets", timestamps[1] + timedelta(seconds=1))
    assert {r["wallet_id"]: r["balance"] for r in rows}["w_alice_usd"] == \
        Decimal("40.0000")

    # After the last revision -> 20
    rows = state_at(conn, "wallets", timestamps[2] + timedelta(seconds=1))
    assert {r["wallet_id"]: r["balance"] for r in rows}["w_alice_usd"] == \
        Decimal("20.0000")


def test_state_at_respects_valid_from_lower_bound(seeded):
    conn, _ = seeded
    # Long before the first event — wallet table should have been empty
    rows = state_at(conn, "wallets", datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert rows == []


def test_state_at_excludes_deleted_rows(seeded):
    conn, capture = seeded
    delete_ts = _ts(0)
    capture.delete(
        "transfers", "tx_pending_002",
        before={"transfer_id": "tx_pending_002"},
        captured_at=delete_ts,
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    # After the delete, snapshot must not include the deleted PK.
    rows = state_at(conn, "transfers", delete_ts + timedelta(seconds=1))
    pks = {r["transfer_id"] for r in rows}
    assert "tx_pending_002" not in pks

    # Before the delete, snapshot must include the deleted PK.
    rows = state_at(conn, "transfers", delete_ts - timedelta(seconds=1))
    pks = {r["transfer_id"] for r in rows}
    assert "tx_pending_002" in pks


# ── restore current state ────────────────────────────────────────────────────


def test_restore_overwrites_current_state(seeded):
    conn, _ = seeded
    timestamps = _evolve_alice_balance(
        seeded, [Decimal("60.0000"), Decimal("40.0000")]
    )

    # Roll wh_wallets back to the moment between the two updates
    n = restore_current_state_to(
        conn, "wallets", timestamps[0] + timedelta(seconds=1)
    )
    assert n >= 1

    bal = conn.execute(
        "SELECT balance FROM wh_wallets WHERE wallet_id = 'w_alice_usd'"
    ).fetchone()[0]
    assert bal == Decimal("60.0000")


def test_restore_does_not_touch_history(seeded):
    conn, _ = seeded
    timestamps = _evolve_alice_balance(
        seeded, [Decimal("60.0000"), Decimal("40.0000")]
    )
    n_hist_before = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets_history"
    ).fetchone()[0]

    restore_current_state_to(conn, "wallets", timestamps[0])
    n_hist_after = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets_history"
    ).fetchone()[0]

    assert n_hist_before == n_hist_after


def test_restore_to_pre_history_results_in_empty_table(seeded):
    conn, _ = seeded
    n = restore_current_state_to(
        conn, "wallets", datetime(2020, 1, 1, tzinfo=timezone.utc)
    )
    assert n == 0
    cnt = conn.execute("SELECT COUNT(*) FROM wh_wallets").fetchone()[0]
    assert cnt == 0


def test_restored_state_accepts_new_cdc_records(seeded):
    """
    After restore, _cdc_seq is reset to 0; the next CDC event (with seq > 0)
    must apply cleanly without being rejected as out-of-order.
    """
    conn, capture = seeded
    timestamps = _evolve_alice_balance(seeded, [Decimal("60.0000")])
    restore_current_state_to(conn, "wallets", timestamps[0])

    # Now push a fresh update — it should land.
    capture.update(
        "wallets", "w_alice_usd",
        before={"wallet_id": "w_alice_usd", "balance": Decimal("60.0000")},
        after={
            "wallet_id":   "w_alice_usd",
            "customer_id": "c_alice",
            "currency":    "USD",
            "balance":     Decimal("12.3456"),
            "status":      "active",
            "created_at":  datetime(2026, 6, 1, 9, tzinfo=timezone.utc),
            "updated_at":  _ts(60),
        },
        captured_at=_ts(60),
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    bal = conn.execute(
        "SELECT balance FROM wh_wallets WHERE wallet_id = 'w_alice_usd'"
    ).fetchone()[0]
    assert bal == Decimal("12.3456")


# ── parity: state_at over multiple tables stays consistent ───────────────────


def test_state_at_customers_count_matches_seed(seeded):
    conn, _ = seeded
    rows = state_at(conn, "customers", _ts(99999))
    assert len(rows) == 3


def test_state_at_ledger_entries_includes_funding_entries(seeded):
    conn, _ = seeded
    rows = state_at(conn, "ledger_entries", _ts(99999))
    funding = [r for r in rows if r["transfer_id"] is None]
    assert len(funding) == 3
