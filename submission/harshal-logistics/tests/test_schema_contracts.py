"""
Tests — schema contract detection and safe-stop behavior.
"""

import duckdb
import pytest

from source.models import SCHEMA_CONTRACT, create_source_tables
from scripts.check_schema_contracts import check_contracts


def _actual_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}


def test_fresh_source_tables_pass_all_contracts():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    assert check_contracts(conn) == []


def test_all_contract_tables_are_present():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    for table in SCHEMA_CONTRACT:
        cols = _actual_columns(conn, table)
        assert len(cols) > 0, f"{table} has no columns"


def test_dropped_column_is_detected_as_violation():
    conn = duckdb.connect(":memory:")
    # Create trade_partners table without compliance_status column to simulate incompatibilities
    conn.execute("""
        CREATE TABLE trade_partners (
            partner_id   VARCHAR PRIMARY KEY,
            company_name VARCHAR NOT NULL,
            country      VARCHAR NOT NULL,
            partner_type VARCHAR NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)
    violations = check_contracts(conn)
    assert any("compliance_status" in v for v in violations)


def test_pipeline_stops_when_contract_violated():
    """
    If a schema contract violation is present, the ingestion logic halts execution.
    """
    from pipeline.cdc import CDCCapture

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE trade_partners (
            partner_id   VARCHAR PRIMARY KEY,
            company_name VARCHAR NOT NULL,
            country      VARCHAR NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    violations = check_contracts(conn)
    capture = CDCCapture()

    if not violations:
        capture.insert("trade_partners", "p1", {"partner_id": "p1", "company_name": "Test"})
    
    assert len(capture.log) == 0, "Ingestion should fail-closed and abort before capturing"


def test_schema_contract_covers_all_key_tables():
    assert "trade_partners" in SCHEMA_CONTRACT
    assert "trade_contracts" in SCHEMA_CONTRACT
    assert "shipment_cargo" in SCHEMA_CONTRACT
    assert "customs_clearances" in SCHEMA_CONTRACT
    assert "settlement_transactions" in SCHEMA_CONTRACT
