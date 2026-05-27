"""Schema contract and fingerprint tests."""

import duckdb

from pipeline.schema_check import (
    check_contract_columns,
    compare_fingerprints,
    contract_spec_to_information_schema_rows,
    load_source_contract_spec,
    validate_live_against_source_contract,
)
from source.models import SCHEMA_CONTRACT, create_source_tables


def test_fresh_source_passes_contracts():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    actual = {
        t: {row[0] for row in conn.execute(f"DESCRIBE {t}").fetchall()}
        for t in SCHEMA_CONTRACT
    }
    assert check_contract_columns(actual) == []


def test_dropped_column_detected():
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    conn.execute("ALTER TABLE customers DROP COLUMN email")
    actual = {
        t: {row[0] for row in conn.execute(f"DESCRIBE {t}").fetchall()}
        for t in SCHEMA_CONTRACT
    }
    violations = check_contract_columns(actual)
    assert any("email" in v for v in violations)


def test_fingerprint_drift_detected():
    baseline = {"customers": "abc123"}
    current = {"customers": "different"}
    assert compare_fingerprints(baseline, current)


def test_validate_schema_stops_on_drift():
    spec = load_source_contract_spec()
    rows = contract_spec_to_information_schema_rows(spec)
    # Simulate source dropping a column
    rows = [r for r in rows if r["COLUMN_NAME"] != "email"]
    violations = validate_live_against_source_contract(rows)
    assert any("email" in v or "missing" in v for v in violations)


def test_live_matching_contract_passes():
    spec = load_source_contract_spec()
    rows = contract_spec_to_information_schema_rows(spec)
    assert validate_live_against_source_contract(rows) == []


def test_pipeline_stops_on_contract_violation():
    from pipeline.cdc_event import CDCEvent

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE customers (
            customer_id BIGINT PRIMARY KEY,
            name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            is_deleted BOOLEAN NOT NULL DEFAULT false
        )
    """)
    actual = {
        "customers": {row[0] for row in conn.execute("DESCRIBE customers").fetchall()}
    }
    violations = check_contract_columns(actual)
    events: list[CDCEvent] = []
    if not violations:
        events.append(
            CDCEvent(
                1,
                "customers",
                "I",
                "{}",
                "{}",
                __import__("datetime").datetime.utcnow(),
            )
        )
    assert len(events) == 0
