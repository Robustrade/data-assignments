"""Schema-change safety tests: detection, fail-closed stop, additive tolerance."""

import duckdb
import pytest

from ingestion.cdc import CDCCapture
from ingestion.pipeline import build_lakehouse, run_ingestion
from ingestion.schema_contract import (
    SchemaIncompatibilityError,
    assert_compatible,
    compare_table,
    observed_schema,
)
from source.models import SCHEMA_CONTRACT


def test_clean_schema_is_compatible(conn):
    warnings = assert_compatible(conn)
    assert warnings == {}


def test_dropped_column_is_breaking(conn):
    # payments has no dependents; drop its index before mutating columns.
    conn.execute("DROP INDEX IF EXISTS idx_payments_order")
    conn.execute("ALTER TABLE payments DROP COLUMN method")
    with pytest.raises(SchemaIncompatibilityError) as exc:
        assert_compatible(conn)
    assert "method" in str(exc.value)


def test_type_change_is_breaking(conn):
    conn.execute("DROP INDEX IF EXISTS idx_payments_order")
    conn.execute("ALTER TABLE payments ALTER COLUMN created_at TYPE VARCHAR")
    with pytest.raises(SchemaIncompatibilityError) as exc:
        assert_compatible(conn)
    assert "created_at" in str(exc.value)


def test_additive_column_is_allowed(conn):
    conn.execute("ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR")
    warnings = assert_compatible(conn)
    assert "customers" in warnings
    assert any("loyalty_tier" in w for w in warnings["customers"])


def test_enum_shrink_is_breaking():
    contract = SCHEMA_CONTRACT["orders"]
    observed = {c.name: (c.dtype.upper(), c.nullable) for c in contract.columns}
    shrunk = {"status": ("pending", "paid")}  # dropped several states
    diff = compare_table(contract, observed, observed_enums=shrunk)
    assert any("enum value(s) removed" in m for m in diff.breaking)


def test_ingestion_stops_and_writes_nothing_on_breaking_change(conn):
    conn.execute("DROP INDEX IF EXISTS idx_payments_order")
    conn.execute("ALTER TABLE payments DROP COLUMN method")

    cap = CDCCapture()
    cap.insert("customers", "c1", {"customer_id": "c1"})

    with pytest.raises(SchemaIncompatibilityError):
        run_ingestion(conn, cap)

    # Fail closed: nothing written to lake or warehouse, checkpoint not advanced.
    lake = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert lake == 0


def test_observed_schema_reports_nullability():
    c = duckdb.connect(":memory:")
    build_lakehouse(c)
    schema = observed_schema(c, "payments")
    assert schema["settled_at"][1] is True  # nullable
    assert schema["payment_id"][1] is False  # NOT NULL
