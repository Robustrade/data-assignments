"""
Tests — warehouse modeling: current-state + SCD2 history.

Covers:
  - inserts land in current-state and history
  - updates upsert current-state and close+open history rows
  - deletes soft-delete current-state and append a delete history row
  - duplicate / replayed records are no-ops
  - out-of-order events do not clobber newer state (sequence guard)
  - SCD2 row count grows by 1 per applied event
  - exactly one row is _is_current=true per PK at any time
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pipeline.cdc import CDCRecord
from pipeline.orchestrator import run_tick
from pipeline.warehouse import apply_cdc_records


def _t(seconds: int = 0) -> datetime:
    base = datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc)
    return base.replace(second=seconds % 60)


# ── inserts propagate ────────────────────────────────────────────────────────


def test_insert_appears_in_warehouse(seeded):
    conn, _ = seeded
    row = conn.execute(
        "SELECT full_name, country_code "
        "FROM wh_customers WHERE customer_id = 'c_alice'"
    ).fetchone()
    assert row == ("Alice Anderson", "US")


def test_all_warehouse_tables_populated_after_seed(seeded):
    conn, _ = seeded
    counts = {
        t: conn.execute(f"SELECT COUNT(*) FROM wh_{t}").fetchone()[0]
        for t in (
            "customers",
            "payment_methods",
            "wallets",
            "transfers",
            "transfer_events",
            "ledger_entries",
        )
    }
    assert counts == {
        "customers":      3,
        "payment_methods": 3,
        "wallets":         4,
        "transfers":       3,
        "transfer_events": 5,
        "ledger_entries":  5,
    }


def test_insert_creates_current_history_row(seeded):
    conn, _ = seeded
    row = conn.execute(
        "SELECT _is_current, _op FROM wh_customers_history "
        "WHERE customer_id = 'c_alice'"
    ).fetchone()
    assert row == (True, "insert")


# ── updates ──────────────────────────────────────────────────────────────────


def test_update_overwrites_current_state(seeded):
    conn, capture = seeded
    capture.update(
        "customers", "c_alice",
        before={"customer_id": "c_alice", "status": "active"},
        after={
            "customer_id":       "c_alice",
            "full_name":         "Alice A. Anderson",
            "email":             "alice@example.invalid",
            "country_code":      "US",
            "status":            "suspended",
            "registration_date": "2024-01-15",
            "created_at":        _t(),
            "updated_at":        _t(),
        },
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    row = conn.execute(
        "SELECT full_name, status FROM wh_customers "
        "WHERE customer_id = 'c_alice'"
    ).fetchone()
    assert row == ("Alice A. Anderson", "suspended")


def test_update_closes_old_history_row_and_opens_new(seeded):
    conn, capture = seeded
    capture.update(
        "wallets", "w_alice_usd",
        before={"wallet_id": "w_alice_usd", "balance": Decimal("75.0000")},
        after={
            "wallet_id":   "w_alice_usd",
            "customer_id": "c_alice",
            "currency":    "USD",
            "balance":     Decimal("60.0000"),
            "status":      "active",
            "created_at":  _t(),
            "updated_at":  _t(1),
        },
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    rows = conn.execute(
        """
        SELECT _is_current, _op, balance
        FROM   wh_wallets_history
        WHERE  wallet_id = 'w_alice_usd'
        ORDER BY _valid_from
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] is False                 # old version closed
    assert rows[1][0] is True                  # new version current
    assert rows[1][2] == Decimal("60.0000")


def test_only_one_current_history_row_per_pk(seeded):
    conn, _ = seeded
    rows = conn.execute(
        """
        SELECT wallet_id, COUNT(*)
        FROM   wh_wallets_history
        WHERE  _is_current = TRUE
        GROUP BY wallet_id
        HAVING COUNT(*) <> 1
        """
    ).fetchall()
    assert rows == []


# ── deletes ──────────────────────────────────────────────────────────────────


def test_delete_marks_current_state_deleted(seeded):
    conn, capture = seeded
    capture.delete(
        "transfers", "tx_pending_002",
        before={"transfer_id": "tx_pending_002"},
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    deleted = conn.execute(
        "SELECT _deleted FROM wh_transfers WHERE transfer_id = 'tx_pending_002'"
    ).fetchone()[0]
    assert deleted is True


def test_delete_appends_history_row_with_op_delete(seeded):
    conn, capture = seeded
    capture.delete(
        "payment_methods", "pm_chen_upi",
        before={"payment_method_id": "pm_chen_upi"},
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    last = conn.execute(
        """
        SELECT _op, _is_current
        FROM   wh_payment_methods_history
        WHERE  payment_method_id = 'pm_chen_upi'
        ORDER BY _valid_from DESC LIMIT 1
        """
    ).fetchone()
    assert last == ("delete", False)


def test_delete_does_not_remove_current_row(seeded):
    conn, capture = seeded
    capture.delete(
        "customers", "c_alice", before={"customer_id": "c_alice"}
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    row = conn.execute(
        "SELECT _deleted FROM wh_customers WHERE customer_id = 'c_alice'"
    ).fetchone()
    assert row is not None
    assert row[0] is True


# ── replay safety / idempotency ──────────────────────────────────────────────


def test_replay_does_not_double_apply(seeded):
    conn, capture = seeded
    n_before = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    n_hist_before = conn.execute(
        "SELECT COUNT(*) FROM wh_customers_history"
    ).fetchone()[0]

    apply_cdc_records(conn, capture.records_since(0))
    n_after = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    n_hist_after = conn.execute(
        "SELECT COUNT(*) FROM wh_customers_history"
    ).fetchone()[0]

    # Current-state count unchanged.
    assert n_after == n_before
    # History count unchanged because sequence guard rejects duplicates.
    assert n_hist_after == n_hist_before


def test_orchestrator_run_tick_is_idempotent(seeded, detector):
    conn, capture = seeded
    n_lake_before = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    stats = run_tick(conn, capture, detector=detector)
    n_lake_after = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    # Already applied — second tick should be a no-op
    assert stats["lake_written"] == 0
    assert stats["warehouse_applied"] == 0
    assert n_lake_before == n_lake_after


# ── out-of-order arrival ─────────────────────────────────────────────────────


@pytest.mark.parametrize("scenario", ["older_arrives_late"])
def test_older_event_does_not_clobber_newer_state(seeded, scenario):
    """
    If a new sequence has already been applied, a later-arriving event with
    a smaller sequence must NOT modify the current-state row.
    """
    conn, capture = seeded
    # Take the latest applied seq.
    latest = capture.latest_sequence
    # Forge an out-of-order CDC record with a lower sequence number
    stale = CDCRecord(
        operation="update",
        table="customers",
        primary_key="c_alice",
        before={"customer_id": "c_alice", "full_name": "Alice Anderson"},
        after={
            "customer_id":       "c_alice",
            "full_name":         "Stale Name",
            "email":             "alice@example.invalid",
            "country_code":      "US",
            "status":            "active",
            "registration_date": "2024-01-15",
            "created_at":        _t(),
            "updated_at":        _t(),
        },
        sequence=1,   # smaller than what we already applied
    )
    apply_cdc_records(conn, [stale])

    name = conn.execute(
        "SELECT full_name FROM wh_customers WHERE customer_id = 'c_alice'"
    ).fetchone()[0]
    assert name == "Alice Anderson"  # original, not 'Stale Name'
    _ = latest
