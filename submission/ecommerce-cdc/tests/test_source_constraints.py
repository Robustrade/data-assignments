"""Modeling & constraint tests for the source layer."""

import duckdb
import pytest

from source.models import SCHEMA_CONTRACT, create_source_tables


@pytest.fixture()
def source() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    return c


def _insert_customer(conn, cid="c1", status="active"):
    conn.execute(
        "INSERT INTO customers VALUES (?, 'A', 'a@x.com', ?, now(), now())",
        [cid, status],
    )


def test_primary_key_uniqueness(source):
    _insert_customer(source, "c1")
    with pytest.raises(duckdb.ConstraintException):
        _insert_customer(source, "c1")


def test_not_null_enforced(source):
    with pytest.raises(duckdb.ConstraintException):
        source.execute(
            "INSERT INTO customers VALUES ('c2', NULL, 'a@x.com', "
            "'active', now(), now())"
        )


def test_enum_check_constraint(source):
    with pytest.raises(duckdb.ConstraintException):
        _insert_customer(source, "c3", status="banana")


def test_foreign_key_reference(source):
    with pytest.raises(duckdb.ConstraintException):
        source.execute(
            "INSERT INTO orders VALUES "
            "('o1', 'ghost', 'pending', 10.00, 'USD', now(), now())"
        )


def test_positive_amount_constraints(source):
    _insert_customer(source, "c1")
    with pytest.raises(duckdb.ConstraintException):
        source.execute(
            "INSERT INTO products VALUES "
            "('p1', 'sku', 'x', -1.00, 'active', now(), now())"
        )


def test_contract_matches_created_columns(source):
    for table, contract in SCHEMA_CONTRACT.items():
        described = {row[0] for row in source.execute(f"DESCRIBE {table}").fetchall()}
        for col in contract.column_names:
            assert col in described, f"{table}.{col} missing from source DDL"
