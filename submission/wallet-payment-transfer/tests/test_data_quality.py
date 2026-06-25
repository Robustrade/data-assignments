"""
Tests — data quality / validation parity.

For every rule defined in scripts/run_data_quality_checks.py we verify
that the seeded warehouse passes; we also seed a deliberately broken
state for a representative subset to confirm each rule actually fires.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pipeline.cdc import CDCCapture
from pipeline.orchestrator import bootstrap, run_tick
from scripts.run_data_quality_checks import all_rules, run_checks


def _ts() -> datetime:
    return datetime(2026, 6, 5, 12, tzinfo=timezone.utc)


# ── happy path ───────────────────────────────────────────────────────────────


def test_seeded_warehouse_passes_all_rules(seeded):
    conn, _ = seeded
    failures = run_checks(conn)
    assert failures == [], (
        "Seeded warehouse should pass all rules. Failures:\n  "
        + "\n  ".join(f"{r.rule_id}: {n}" for r, n in failures)
    )


def test_rules_cover_all_categories():
    cats = {r.category for r in all_rules()}
    assert {"system", "business"}.issubset(cats)


# ── system rule firing ───────────────────────────────────────────────────────


def test_pk_uniqueness_rule_fires_on_duplicate(seeded):
    conn, _ = seeded
    # Force a duplicate in wh_customers by direct INSERT
    conn.execute(
        "INSERT INTO wh_customers VALUES "
        "('c_alice', 'Dup', 'd@x.test', 'US', 'active', "
        "DATE '2024-01-15', ?, ?, 999, FALSE)",
        [_ts(), _ts()],
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "sys.pk_unique.customers" in rule_ids


def test_enum_rule_fires_on_unknown_value(seeded):
    conn, _ = seeded
    conn.execute(
        "UPDATE wh_customers SET status = 'banned' WHERE customer_id = 'c_alice'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "sys.enum.customers.status" in rule_ids


def test_fk_rule_fires_on_orphan(seeded):
    conn, _ = seeded
    conn.execute(
        "UPDATE wh_wallets SET customer_id = 'c_missing' "
        "WHERE wallet_id = 'w_alice_usd'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "sys.fk.wallets.customer_id" in rule_ids


# ── business rule firing ─────────────────────────────────────────────────────


def test_negative_balance_rule_fires(seeded):
    conn, _ = seeded
    conn.execute(
        "UPDATE wh_wallets SET balance = -1.0000 "
        "WHERE wallet_id = 'w_bob_gbp'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "biz.wallet.balance_non_negative" in rule_ids


def test_settled_at_after_created_rule_fires(seeded):
    conn, _ = seeded
    conn.execute(
        "UPDATE wh_transfers SET settled_at = TIMESTAMP '2020-01-01' "
        "WHERE transfer_id = 'tx_settled_001'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "biz.transfer.settled_at_after_created" in rule_ids


def test_ledger_signed_amount_consistency_rule_fires(seeded):
    conn, _ = seeded
    conn.execute(
        "UPDATE wh_ledger_entries SET signed_amount = 99 "
        "WHERE ledger_entry_id = 'le_001_debit'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "biz.ledger.signed_amount_consistent" in rule_ids


def test_ledger_reconciles_to_wallet_balance_rule_fires(seeded):
    conn, _ = seeded
    # Break the reconciliation: bump wallet balance away from ledger sum.
    conn.execute(
        "UPDATE wh_wallets SET balance = balance + 1 "
        "WHERE wallet_id = 'w_alice_usd'"
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "biz.ledger.reconciles_to_wallet_balance" in rule_ids


def test_transfer_status_transition_rule_fires(seeded):
    conn, _ = seeded
    # Insert an illegal transition: settled -> pending (not allowed)
    conn.execute(
        "INSERT INTO wh_transfer_events VALUES ("
        "'te_bad', 'tx_settled_001', 'settled', 'pending', "
        "'illegal-transition', ?, 999, FALSE)",
        [_ts()],
    )
    failures = run_checks(conn)
    rule_ids = {r.rule_id for r, _ in failures}
    assert "biz.transfer_event.allowed_transition" in rule_ids


# ── parity sanity: every contract column with NOT NULL has a rule ────────────


def test_every_contract_not_null_has_a_warehouse_rule():
    rule_ids = {r.rule_id for r in all_rules()}
    from source.contracts import CONTRACTS
    for table, contract in CONTRACTS.items():
        for col in contract.columns:
            if col.nullable:
                continue
            expected = f"sys.not_null.{table}.{col.name}"
            assert expected in rule_ids, (
                f"missing parity rule {expected} — every NOT NULL "
                "contract column must be checked downstream"
            )


# ── soft-deletes are excluded from quality rules ──────────────────────────────


def test_deleted_rows_do_not_trigger_validation_failures(seeded):
    conn, capture = seeded
    capture.delete(
        "transfers", "tx_pending_002",
        before={"transfer_id": "tx_pending_002"},
    )
    run_tick(conn, capture)
    # _deleted=true rows must be ignored by every rule.
    failures = run_checks(conn)
    assert failures == []
