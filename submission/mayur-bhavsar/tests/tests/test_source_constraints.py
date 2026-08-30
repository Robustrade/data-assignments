"""Modelling and constraint tests.

The source is the system of record; if its constraints do not actually hold,
every guarantee downstream is built on sand. These tests assert the constraints
by trying to violate them.
"""

from __future__ import annotations

import sqlite3

import pytest

from cdclake import contracts as contracts_mod
from cdclake import source as source_mod
from cdclake.config import TRACKED_TABLES, WEAK_ENTITIES


def test_primary_keys_are_enforced(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.insert(
            seeded_platform.conn,
            "customer",
            customer_id=1,  # already exists
            email="dup@example.com",
            full_name="Duplicate",
            status="ACTIVE",
            phone=None,
            created_at="2026-08-01T09:00:00.000Z",
            updated_at="2026-08-01T09:00:00.000Z",
        )


def test_unique_constraints_are_enforced(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.insert(
            seeded_platform.conn,
            "customer",
            customer_id=99,
            email="ada@example.com",  # already taken
            full_name="Impostor",
            status="ACTIVE",
            phone=None,
            created_at="2026-08-01T09:00:00.000Z",
            updated_at="2026-08-01T09:00:00.000Z",
        )


def test_foreign_keys_are_enforced(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.insert(
            seeded_platform.conn,
            "customer_order",
            order_id=9999,
            customer_id=424242,  # no such customer
            status="PENDING",
            currency="GBP",
            total_amount=10.00,
            placed_at="2026-08-02T09:00:00.000Z",
            cancelled_at=None,
            created_at="2026-08-02T09:00:00.000Z",
            updated_at="2026-08-02T09:00:00.000Z",
        )


def test_enum_domain_is_enforced(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.update(
            seeded_platform.conn,
            "customer",
            {"customer_id": 1},
            status="NOT_A_REAL_STATUS",
            updated_at="2026-08-02T09:00:00.000Z",
        )


def test_monetary_amounts_cannot_be_negative(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.update(
            seeded_platform.conn,
            "customer_order",
            {"order_id": 1001},
            total_amount=-1.00,
            updated_at="2026-08-02T09:00:00.000Z",
        )


def test_settlement_cannot_precede_creation(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.update(
            seeded_platform.conn,
            "payment",
            {"payment_id": 10010},
            status="SETTLED",
            settled_at="2020-01-01T00:00:00.000Z",
            updated_at="2026-08-02T09:00:00.000Z",
        )


def test_cancellation_timestamp_requires_cancelled_status(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.update(
            seeded_platform.conn,
            "customer_order",
            {"order_id": 1001},
            cancelled_at="2026-08-05T09:00:00.000Z",
            updated_at="2026-08-05T09:00:00.000Z",
        )


def test_line_extended_amount_must_be_consistent(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.update(
            seeded_platform.conn,
            "order_item",
            {"order_id": 1001, "line_no": 1},
            extended_amount=0.01,
            updated_at="2026-08-02T09:00:00.000Z",
        )


def test_successful_attempt_may_not_carry_a_failure_code(seeded_platform):
    with pytest.raises(sqlite3.IntegrityError):
        source_mod.insert(
            seeded_platform.conn,
            "payment_attempt",
            payment_id=10010,
            attempt_no=1,
            result="SUCCESS",
            failure_code="51",
            attempted_at="2026-08-03T09:00:00.000Z",
            created_at="2026-08-03T09:00:00.000Z",
            updated_at="2026-08-03T09:00:00.000Z",
        )


def test_weak_entities_have_composite_keys(seeded_platform):
    """A weak entity's identity must depend on its parent."""
    for table in WEAK_ENTITIES:
        contract = contracts_mod.introspect_table(seeded_platform.conn, table)
        assert (
            len(contract.primary_key) > 1
        ), f"{table} is modelled as a weak entity but has a single-column key"
        parents = {fk.references_table for fk in contract.foreign_keys}
        assert parents, f"{table} has no identifying parent"


def test_indexes_exist_for_the_documented_access_patterns(seeded_platform):
    """Indexes are part of the contract, not incidental."""
    expected = {
        "customer_order": {
            "idx_customer_order_customer_placed",
            "idx_customer_order_updated",
        },
        "order_item": {"idx_order_item_product"},
        "payment": {"idx_payment_order", "idx_payment_status_created"},
        "payment_attempt": {"idx_payment_attempt_result"},
    }
    for table, indexes in expected.items():
        contract = contracts_mod.introspect_table(seeded_platform.conn, table)
        assert indexes <= set(contract.indexes), f"{table} is missing {indexes}"


def test_every_tracked_table_has_a_published_contract():
    for table in TRACKED_TABLES:
        contract = contracts_mod.load_contract(table)
        assert contract.columns
        assert contract.primary_key
        assert contract.description, f"{table} contract publishes no description"
