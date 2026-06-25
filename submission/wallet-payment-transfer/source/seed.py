"""
Representative seed data for the wallet domain.

Used by:
  - tests (via the `seeded` fixture)
  - scripts/run_pipeline.py (end-to-end demo)

The seed builds a small but coherent slice of the system:
  - 3 customers
  - 3 payment methods (one per customer)
  - 4 wallets (Alice holds two — USD + EUR)
  - 3 transfers in different states (settled / pending / failed)
  - matching transfer_events
  - ledger_entries that *reconcile* to each wallet.balance per the
    monetary integrity invariant SUM(signed_amount) == balance.

Funding ledger entries (transfer_id=NULL) seed the wallet starting
balance; the remaining ledger entries flow from a real transfer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


@dataclass(frozen=True)
class SeedRow:
    table: str
    pk: str
    data: dict


def build_seed_rows() -> list[SeedRow]:
    """
    Return ordered list of seed rows. Order matters: parents first so
    foreign keys are valid as each row is applied.

    Final reconciled state per wallet (verified by data quality checks):
        w_alice_usd : balance = 75   (funded 100, debited 25)
        w_alice_eur : balance = 0    (never funded)
        w_bob_gbp   : balance = 50   (funded 50)
        w_chen_usd  : balance = 50   (funded 25, credited 25)
    """
    rows: list[SeedRow] = []
    t0 = _ts(2026, 6, 1, 9, 0)

    # ── customers ────────────────────────────────────────────────────────────
    rows.append(SeedRow("customers", "c_alice", {
        "customer_id":       "c_alice",
        "full_name":         "Alice Anderson",
        "email":             "alice@example.invalid",
        "country_code":      "US",
        "status":            "active",
        "registration_date": date(2024, 1, 15),
        "created_at":        t0,
        "updated_at":        t0,
    }))
    rows.append(SeedRow("customers", "c_bob", {
        "customer_id":       "c_bob",
        "full_name":         "Bob Brown",
        "email":             "bob@example.invalid",
        "country_code":      "GB",
        "status":            "active",
        "registration_date": date(2024, 3, 20),
        "created_at":        t0,
        "updated_at":        t0,
    }))
    rows.append(SeedRow("customers", "c_chen", {
        "customer_id":       "c_chen",
        "full_name":         "Chen Cheng",
        "email":             "chen@example.invalid",
        "country_code":      "US",
        "status":            "active",
        "registration_date": date(2025, 11, 1),
        "created_at":        t0,
        "updated_at":        t0,
    }))

    # ── payment methods ──────────────────────────────────────────────────────
    rows.append(SeedRow("payment_methods", "pm_alice_card", {
        "payment_method_id": "pm_alice_card",
        "customer_id":       "c_alice",
        "method_type":       "card",
        "masked_identifier": "****4242",
        "is_default":        True,
        "status":            "active",
        "created_at":        t0,
        "updated_at":        t0,
    }))
    rows.append(SeedRow("payment_methods", "pm_bob_bank", {
        "payment_method_id": "pm_bob_bank",
        "customer_id":       "c_bob",
        "method_type":       "bank",
        "masked_identifier": "GB**0001",
        "is_default":        True,
        "status":            "active",
        "created_at":        t0,
        "updated_at":        t0,
    }))
    rows.append(SeedRow("payment_methods", "pm_chen_upi", {
        "payment_method_id": "pm_chen_upi",
        "customer_id":       "c_chen",
        "method_type":       "upi",
        "masked_identifier": "chen@upi",
        "is_default":        True,
        "status":            "active",
        "created_at":        t0,
        "updated_at":        t0,
    }))

    # ── wallets (final reconciled balances) ──────────────────────────────────
    rows.append(SeedRow("wallets", "w_alice_usd", {
        "wallet_id":   "w_alice_usd",
        "customer_id": "c_alice",
        "currency":    "USD",
        "balance":     Decimal("75.0000"),     # 100 funded, then -25
        "status":      "active",
        "created_at":  t0,
        "updated_at":  _ts(2026, 6, 1, 10, 1),
    }))
    rows.append(SeedRow("wallets", "w_alice_eur", {
        "wallet_id":   "w_alice_eur",
        "customer_id": "c_alice",
        "currency":    "EUR",
        "balance":     Decimal("0.0000"),
        "status":      "active",
        "created_at":  t0,
        "updated_at":  t0,
    }))
    rows.append(SeedRow("wallets", "w_bob_gbp", {
        "wallet_id":   "w_bob_gbp",
        "customer_id": "c_bob",
        "currency":    "GBP",
        "balance":     Decimal("50.0000"),
        "status":      "active",
        "created_at":  t0,
        "updated_at":  t0,
    }))
    rows.append(SeedRow("wallets", "w_chen_usd", {
        "wallet_id":   "w_chen_usd",
        "customer_id": "c_chen",
        "currency":    "USD",
        "balance":     Decimal("50.0000"),     # 25 funded, then +25
        "status":      "active",
        "created_at":  t0,
        "updated_at":  _ts(2026, 6, 1, 10, 1),
    }))

    # ── transfers ────────────────────────────────────────────────────────────
    rows.append(SeedRow("transfers", "tx_settled_001", {
        "transfer_id":       "tx_settled_001",
        "from_wallet_id":    "w_alice_usd",
        "to_wallet_id":      "w_chen_usd",
        "payment_method_id": "pm_alice_card",
        "amount":            Decimal("25.0000"),
        "currency":          "USD",
        "status":            "settled",
        "reference":         "rent-share",
        "failure_reason":    None,
        "created_at":        _ts(2026, 6, 1, 10, 0),
        "settled_at":        _ts(2026, 6, 1, 10, 1),
    }))
    rows.append(SeedRow("transfers", "tx_pending_002", {
        "transfer_id":       "tx_pending_002",
        "from_wallet_id":    "w_bob_gbp",
        "to_wallet_id":      "w_alice_eur",
        "payment_method_id": "pm_bob_bank",
        "amount":            Decimal("10.0000"),
        "currency":          "GBP",
        "status":            "pending",
        "reference":         None,
        "failure_reason":    None,
        "created_at":        _ts(2026, 6, 1, 11, 0),
        "settled_at":        None,
    }))
    rows.append(SeedRow("transfers", "tx_failed_003", {
        "transfer_id":       "tx_failed_003",
        "from_wallet_id":    "w_chen_usd",
        "to_wallet_id":      "w_bob_gbp",
        "payment_method_id": "pm_chen_upi",
        "amount":            Decimal("5.0000"),
        "currency":          "USD",
        "status":            "failed",
        "reference":         "groceries",
        "failure_reason":    "insufficient-funds",
        "created_at":        _ts(2026, 6, 1, 12, 0),
        "settled_at":        None,
    }))

    # ── transfer_events ──────────────────────────────────────────────────────
    rows.append(SeedRow("transfer_events", "te_001_a", {
        "transfer_event_id": "te_001_a",
        "transfer_id":       "tx_settled_001",
        "from_status":       None,
        "to_status":         "pending",
        "note":              "created",
        "occurred_at":       _ts(2026, 6, 1, 10, 0),
    }))
    rows.append(SeedRow("transfer_events", "te_001_b", {
        "transfer_event_id": "te_001_b",
        "transfer_id":       "tx_settled_001",
        "from_status":       "pending",
        "to_status":         "settled",
        "note":              "card-charged",
        "occurred_at":       _ts(2026, 6, 1, 10, 1),
    }))
    rows.append(SeedRow("transfer_events", "te_002_a", {
        "transfer_event_id": "te_002_a",
        "transfer_id":       "tx_pending_002",
        "from_status":       None,
        "to_status":         "pending",
        "note":              "created",
        "occurred_at":       _ts(2026, 6, 1, 11, 0),
    }))
    rows.append(SeedRow("transfer_events", "te_003_a", {
        "transfer_event_id": "te_003_a",
        "transfer_id":       "tx_failed_003",
        "from_status":       None,
        "to_status":         "pending",
        "note":              "created",
        "occurred_at":       _ts(2026, 6, 1, 12, 0),
    }))
    rows.append(SeedRow("transfer_events", "te_003_b", {
        "transfer_event_id": "te_003_b",
        "transfer_id":       "tx_failed_003",
        "from_status":       "pending",
        "to_status":         "failed",
        "note":              "insufficient-funds",
        "occurred_at":       _ts(2026, 6, 1, 12, 1),
    }))

    # ── ledger_entries (funding entries + transfer-driven movements) ─────────
    # Funding for w_alice_usd: +100
    rows.append(SeedRow("ledger_entries", "le_alice_fund", {
        "ledger_entry_id": "le_alice_fund",
        "wallet_id":       "w_alice_usd",
        "transfer_id":     None,
        "direction":       "credit",
        "amount":          Decimal("100.0000"),
        "signed_amount":   Decimal("100.0000"),
        "balance_after":   Decimal("100.0000"),
        "occurred_at":     t0,
    }))
    # Funding for w_bob_gbp: +50
    rows.append(SeedRow("ledger_entries", "le_bob_fund", {
        "ledger_entry_id": "le_bob_fund",
        "wallet_id":       "w_bob_gbp",
        "transfer_id":     None,
        "direction":       "credit",
        "amount":          Decimal("50.0000"),
        "signed_amount":   Decimal("50.0000"),
        "balance_after":   Decimal("50.0000"),
        "occurred_at":     t0,
    }))
    # Funding for w_chen_usd: +25
    rows.append(SeedRow("ledger_entries", "le_chen_fund", {
        "ledger_entry_id": "le_chen_fund",
        "wallet_id":       "w_chen_usd",
        "transfer_id":     None,
        "direction":       "credit",
        "amount":          Decimal("25.0000"),
        "signed_amount":   Decimal("25.0000"),
        "balance_after":   Decimal("25.0000"),
        "occurred_at":     t0,
    }))
    # Settled transfer 001: alice debit 25 -> 75
    rows.append(SeedRow("ledger_entries", "le_001_debit", {
        "ledger_entry_id": "le_001_debit",
        "wallet_id":       "w_alice_usd",
        "transfer_id":     "tx_settled_001",
        "direction":       "debit",
        "amount":          Decimal("25.0000"),
        "signed_amount":   Decimal("-25.0000"),
        "balance_after":   Decimal("75.0000"),
        "occurred_at":     _ts(2026, 6, 1, 10, 1),
    }))
    # Settled transfer 001: chen credit 25 -> 50
    rows.append(SeedRow("ledger_entries", "le_001_credit", {
        "ledger_entry_id": "le_001_credit",
        "wallet_id":       "w_chen_usd",
        "transfer_id":     "tx_settled_001",
        "direction":       "credit",
        "amount":          Decimal("25.0000"),
        "signed_amount":   Decimal("25.0000"),
        "balance_after":   Decimal("50.0000"),
        "occurred_at":     _ts(2026, 6, 1, 10, 1),
    }))

    return rows
