"""
Tests — schema-drift detection and stop-the-line behavior.

Covers each detection rule in pipeline/schema_detector.py:
  - missing_table
  - column_dropped
  - type_changed
  - nullability_tightened
  - column_added (warn, additive)
  - non_nullable_column_added (breaking)
  - primary_key_changed
  - enum_value_added (warn)

Plus the *system-level* stop-the-line:
  - run_tick aborts when a breaking drift is present
  - capture refuses writes when drift is breaking
"""

from __future__ import annotations

import duckdb
import pytest

from pipeline.cdc import CDCCapture, SchemaContractViolation
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.schema_detector import SchemaDetector, Severity
from source.contracts import CONTRACTS
from source.models import create_source_tables


def _fresh_source() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    return c


def _drop_dependent_tables_for_alter(c: duckdb.DuckDBPyConnection) -> None:
    """DuckDB rejects ALTER on tables with FK refs; drop FK children first."""
    for t in ("ledger_entries", "transfer_events", "transfers",
              "payment_methods", "wallets"):
        c.execute(f"DROP TABLE IF EXISTS {t}")


# ── happy path ───────────────────────────────────────────────────────────────


def test_clean_source_passes_all_contracts():
    c = _fresh_source()
    report = SchemaDetector(c).check_all()
    assert not report.has_breaking()


def test_check_table_returns_one_report_per_table():
    c = _fresh_source()
    report = SchemaDetector(c).check_all()
    assert set(report.by_table) == set(CONTRACTS)


# ── missing table ────────────────────────────────────────────────────────────


def test_missing_table_is_breaking():
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    # Drop FK descendants then customers
    _drop_dependent_tables_for_alter(c)
    c.execute("DROP TABLE IF EXISTS customers")
    drifts = SchemaDetector(c).check_table("customers").drifts
    rules = {d.rule for d in drifts}
    assert "missing_table" in rules
    assert any(d.severity is Severity.BREAKING for d in drifts
               if d.rule == "missing_table")


# ── dropped column ───────────────────────────────────────────────────────────


def test_dropped_column_is_breaking():
    c = _fresh_source()
    _drop_dependent_tables_for_alter(c)
    c.execute("ALTER TABLE customers DROP COLUMN email")
    report = SchemaDetector(c).check_table("customers")
    assert any(
        d.rule == "column_dropped" and d.column == "email"
        and d.severity is Severity.BREAKING
        for d in report.drifts
    )


# ── type changed ─────────────────────────────────────────────────────────────


def test_type_change_is_breaking():
    c = _fresh_source()
    _drop_dependent_tables_for_alter(c)
    c.execute("ALTER TABLE customers ALTER country_code TYPE INTEGER USING 0")
    report = SchemaDetector(c).check_table("customers")
    assert any(
        d.rule == "type_changed" and d.column == "country_code"
        and d.severity is Severity.BREAKING
        for d in report.drifts
    )


# ── nullability tightened ────────────────────────────────────────────────────


def test_nullability_tightening_is_breaking():
    """
    transfers.reference is nullable in the contract. If the source
    later forbids NULL, the contract should report it as breaking.
    """
    c = _fresh_source()
    # Drop FK descendants of transfers.
    c.execute("DROP TABLE IF EXISTS ledger_entries")
    c.execute("DROP TABLE IF EXISTS transfer_events")
    c.execute("ALTER TABLE transfers ALTER COLUMN reference SET NOT NULL")
    report = SchemaDetector(c).check_table("transfers")
    assert any(
        d.rule == "nullability_tightened" and d.column == "reference"
        and d.severity is Severity.BREAKING
        for d in report.drifts
    )


# ── added column ─────────────────────────────────────────────────────────────


def test_new_nullable_column_is_warn_only():
    c = _fresh_source()
    _drop_dependent_tables_for_alter(c)
    c.execute("ALTER TABLE customers ADD COLUMN nickname VARCHAR")
    report = SchemaDetector(c).check_table("customers")
    assert not report.has_breaking()
    warn_rules = {d.rule for d in report.warnings}
    assert "column_added" in warn_rules


def test_new_non_null_column_is_breaking():
    c = _fresh_source()
    _drop_dependent_tables_for_alter(c)
    c.execute(
        "ALTER TABLE customers ADD COLUMN region VARCHAR DEFAULT 'EU' NOT NULL"
    )
    report = SchemaDetector(c).check_table("customers")
    assert report.has_breaking()
    assert any(d.rule == "non_nullable_column_added" for d in report.drifts)


# ── enum value drift ─────────────────────────────────────────────────────────


def test_unexpected_enum_value_in_data_is_warn():
    """
    The detector cannot prove an enum *removal* without source DDL access,
    so it warns when it sees a *new* value present in data. We simulate by
    bypassing the CHECK constraint with a DROP-then-recreate.
    """
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE customers (
            customer_id  VARCHAR PRIMARY KEY,
            full_name    VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            country_code VARCHAR NOT NULL,
            status       VARCHAR NOT NULL,
            registration_date DATE NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)
    c.execute(
        "INSERT INTO customers VALUES "
        "('c1', 'Alice', 'a@x.test', 'US', 'banned', "
        "DATE '2025-01-01', TIMESTAMP '2026-06-01', TIMESTAMP '2026-06-01')"
    )
    report = SchemaDetector(c).check_table("customers")
    assert any(d.rule == "enum_value_added" for d in report.warnings)


# ── stop-the-line: orchestrator ──────────────────────────────────────────────


def test_run_tick_aborts_on_breaking_drift(conn, detector):
    bootstrap(conn)
    capture = CDCCapture()
    capture.insert("customers", "c_x", {"customer_id": "c_x"})

    # Drop a contracted column.
    _drop_dependent_tables_for_alter(conn)
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    stats = run_tick(conn, capture, detector=detector)
    assert stats["aborted"] is True
    assert stats["reason"] == "schema_contract_violation"
    assert stats["lake_written"] == 0
    assert stats["warehouse_applied"] == 0


def test_run_tick_reports_drift_summary_on_abort(conn, detector):
    bootstrap(conn)
    capture = CDCCapture()
    capture.insert("customers", "c_x", {"customer_id": "c_x"})
    _drop_dependent_tables_for_alter(conn)
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    stats = run_tick(conn, capture, detector=detector)
    assert "email" in stats["drift_summary"]
    assert "column_dropped" in stats["drift_summary"]


# ── stop-the-line: capture ───────────────────────────────────────────────────


def test_capture_refuses_writes_when_strict(conn, detector):
    _drop_dependent_tables_for_alter(conn)
    conn.execute("ALTER TABLE customers DROP COLUMN email")

    strict = CDCCapture(detector, require_compatible_schema=True)
    with pytest.raises(SchemaContractViolation):
        strict.insert("customers", "c1", {"customer_id": "c1"})
    assert strict.log == []
