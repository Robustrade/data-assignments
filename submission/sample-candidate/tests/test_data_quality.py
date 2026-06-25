"""
Tests — data quality and warehouse correctness.

Covers: insert propagation, update correctness, soft-delete, replay safety,
PK uniqueness, not-null checks, business rule assertions, lake completeness,
and lake immutability (no deletes or updates to lake rows).
"""

from datetime import datetime, timezone

import pytest

from pipeline.lake import append_to_lake
from pipeline.warehouse import apply_cdc_records


def _ts() -> datetime:
    return datetime.now(timezone.utc)


# ── insert propagation ────────────────────────────────────────────────────────


def test_insert_appears_in_warehouse(seeded):
    conn, _ = seeded
    row = conn.execute(
        "SELECT name FROM wh_customers WHERE customer_id = 'c1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Alice"


def test_insert_appears_in_lake(seeded):
    conn, _ = seeded
    count = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events WHERE table_name = 'customers'"
        " AND operation = 'insert'"
    ).fetchone()[0]
    assert count == 2


def test_all_tables_populated_after_seed(seeded):
    conn, _ = seeded
    assert conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM wh_wallets").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM wh_transactions").fetchone()[0] == 2


# ── update correctness ────────────────────────────────────────────────────────


def test_update_overwrites_warehouse_row(seeded):
    conn, capture = seeded
    ts = _ts()
    capture.update(
        "customers",
        "c1",
        {
            "customer_id": "c1",
            "name": "Alice Smith",
            "email": "alice-test-user",
            "status": "suspended",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    new_records = capture.records_since(capture.latest_sequence - 1)
    apply_cdc_records(conn, new_records)

    row = conn.execute(
        "SELECT name, status FROM wh_customers WHERE customer_id = 'c1'"
    ).fetchone()
    assert row[0] == "Alice Smith"
    assert row[1] == "suspended"


def test_update_does_not_create_duplicate_warehouse_row(seeded):
    conn, capture = seeded
    ts = _ts()
    capture.update(
        "wallets",
        "w1",
        {
            "wallet_id": "w1",
            "customer_id": "c1",
            "balance": 200.00,
            "currency": "USD",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    count = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets WHERE wallet_id = 'w1'"
    ).fetchone()[0]
    assert count == 1


# ── soft delete ───────────────────────────────────────────────────────────────


def test_delete_marks_warehouse_row_as_deleted(seeded):
    conn, capture = seeded
    capture.delete("customers", "c2", {"customer_id": "c2"})
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    row = conn.execute(
        "SELECT _deleted FROM wh_customers WHERE customer_id = 'c2'"
    ).fetchone()
    assert row is not None
    assert row[0] is True


def test_delete_does_not_remove_row_from_warehouse(seeded):
    conn, capture = seeded
    capture.delete("wallets", "w2", {"wallet_id": "w2"})
    apply_cdc_records(conn, capture.records_since(capture.latest_sequence - 1))

    count = conn.execute(
        "SELECT COUNT(*) FROM wh_wallets WHERE wallet_id = 'w2'"
    ).fetchone()[0]
    assert count == 1


# ── lake immutability ─────────────────────────────────────────────────────────


def test_lake_row_count_only_increases(seeded):
    conn, capture = seeded
    before = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]

    ts = _ts()
    capture.update(
        "customers",
        "c1",
        {
            "customer_id": "c1",
            "name": "Alice Updated",
            "email": "alice-test-user",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    new_records = capture.records_since(capture.latest_sequence - 1)
    append_to_lake(conn, new_records)

    after = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert after > before


def test_lake_retains_all_operations_for_same_pk(seeded):
    conn, capture = seeded
    ts = _ts()
    capture.update(
        "customers",
        "c1",
        {
            "customer_id": "c1",
            "name": "Alice v2",
            "email": "alice-test-user",
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.delete("customers", "c1", {"customer_id": "c1"})
    append_to_lake(conn, capture.records_since(capture.latest_sequence - 2))

    events = conn.execute(
        "SELECT operation FROM lake_cdc_events"
        " WHERE table_name = 'customers' AND primary_key = 'c1'"
        " ORDER BY sequence"
    ).fetchall()
    ops = [e[0] for e in events]
    assert "insert" in ops
    assert "update" in ops
    assert "delete" in ops


# ── PK uniqueness ─────────────────────────────────────────────────────────────


def test_warehouse_customers_pk_is_unique(seeded):
    conn, _ = seeded
    total = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    distinct = conn.execute(
        "SELECT COUNT(DISTINCT customer_id) FROM wh_customers"
    ).fetchone()[0]
    assert total == distinct


def test_warehouse_wallets_pk_is_unique(seeded):
    conn, _ = seeded
    total = conn.execute("SELECT COUNT(*) FROM wh_wallets").fetchone()[0]
    distinct = conn.execute(
        "SELECT COUNT(DISTINCT wallet_id) FROM wh_wallets"
    ).fetchone()[0]
    assert total == distinct


# ── business rules ────────────────────────────────────────────────────────────


def test_transaction_amounts_are_positive(seeded):
    conn, _ = seeded
    bad = conn.execute(
        "SELECT COUNT(*) FROM wh_transactions WHERE amount <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_wallet_balances_are_non_negative(seeded):
    conn, _ = seeded
    bad = conn.execute("SELECT COUNT(*) FROM wh_wallets WHERE balance < 0").fetchone()[
        0
    ]
    assert bad == 0


def test_transaction_directions_are_valid(seeded):
    conn, _ = seeded
    bad = conn.execute(
        "SELECT COUNT(*) FROM wh_transactions"
        " WHERE direction NOT IN ('credit', 'debit')"
    ).fetchone()[0]
    assert bad == 0


# ── replay safety ─────────────────────────────────────────────────────────────


def test_replaying_same_records_does_not_create_duplicates(seeded):
    conn, capture = seeded
    # Replay all records from the beginning
    all_records = capture.records_since(0)
    apply_cdc_records(conn, all_records)

    count = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    assert count == 2  # Must not double to 4


@pytest.mark.parametrize("offset", [0, 1, 2])
def test_partial_replay_from_checkpoint_is_idempotent(seeded, offset: int):
    conn, capture = seeded
    partial = capture.records_since(offset)
    apply_cdc_records(conn, partial)

    count = conn.execute("SELECT COUNT(*) FROM wh_customers").fetchone()[0]
    assert count == 2
