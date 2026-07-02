from __future__ import annotations

import duckdb
import pytest

from pipeline.schema_contracts import IncompatibleSchemaChange, assert_compatible, validate_event_schema
from source.models import SCHEMA_CONTRACT, create_source_tables


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in conn.execute(f"DESCRIBE {table}").fetchall()}


def test_fresh_source_schema_matches_contract():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    violations = []
    for table in SCHEMA_CONTRACT:
        violations.extend(validate_event_schema(table, _columns(conn, table)))

    assert violations == []


def test_dropped_column_is_incompatible():
    columns = dict(SCHEMA_CONTRACT["reservations"]["columns"])
    columns.pop("total_amount")

    with pytest.raises(IncompatibleSchemaChange, match="total_amount"):
        assert_compatible("reservations", columns)


def test_type_change_is_incompatible():
    columns = dict(SCHEMA_CONTRACT["payments"]["columns"])
    columns["amount"] = "VARCHAR"

    violations = validate_event_schema("payments", columns)

    assert any("amount" in violation.message for violation in violations)


def test_nullable_additive_column_is_safe_by_contract_policy():
    columns = dict(SCHEMA_CONTRACT["customers"]["columns"])
    columns["marketing_opt_in"] = "BOOLEAN"

    assert validate_event_schema("customers", columns) == []


def test_unknown_table_stops_ingestion():
    with pytest.raises(IncompatibleSchemaChange, match="not registered"):
        assert_compatible("promo_codes", {"promo_code": "VARCHAR"})

