import duckdb
import pytest

from scripts.check_schema_contracts import check_all, enforce, SchemaContractViolation

# Building each scenario as a standalone in-memory schema (rather than ALTERing
# a real table) sidesteps a real DuckDB limitation: ALTER TABLE DROP/RENAME
# COLUMN refuses to run once a table has an index or is referenced by a FK,
# which ours always are. A live source doing a real migration wouldn't have
# that restriction, so this is the more portable way to prove the check works.
ORDERS_OK = "CREATE TABLE orders (order_id VARCHAR, customer_id VARCHAR, status VARCHAR, total_amount DECIMAL(18,2), discount_code VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)"
ITEMS_OK = "CREATE TABLE order_items (order_item_id VARCHAR, order_id VARCHAR, sku VARCHAR, quantity INTEGER, unit_price DECIMAL(18,2))"
PAYMENTS_OK = "CREATE TABLE payments (payment_id VARCHAR, order_id VARCHAR, amount DECIMAL(18,2), status VARCHAR, created_at TIMESTAMP, paid_at TIMESTAMP)"
CUSTOMERS_OK = "CREATE TABLE customers (customer_id VARCHAR, name VARCHAR, email VARCHAR, status VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)"


def _conn_with_customers(customers_ddl: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(customers_ddl)
    conn.execute(ORDERS_OK)
    conn.execute(ITEMS_OK)
    conn.execute(PAYMENTS_OK)
    return conn


def test_fully_compatible_schema_passes_clean():
    conn = _conn_with_customers(CUSTOMERS_OK)
    enforce(conn)  # must not raise
    assert check_all(conn) == {}


def test_dropped_column_is_detected():
    conn = _conn_with_customers(
        "CREATE TABLE customers (customer_id VARCHAR, email VARCHAR, status VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)"
    )
    with pytest.raises(SchemaContractViolation, match="name"):
        enforce(conn)


def test_renamed_column_is_detected_as_missing():
    conn = _conn_with_customers(
        "CREATE TABLE customers (customer_id VARCHAR, name VARCHAR, email_address VARCHAR, status VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)"
    )
    with pytest.raises(SchemaContractViolation, match="email"):
        enforce(conn)


def test_changed_type_is_detected():
    conn = _conn_with_customers(
        "CREATE TABLE customers (customer_id VARCHAR, name VARCHAR, email VARCHAR, status INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP)"
    )
    with pytest.raises(SchemaContractViolation, match="type changed"):
        enforce(conn)


def test_additive_column_does_not_break_ingestion():
    conn = _conn_with_customers(
        "CREATE TABLE customers (customer_id VARCHAR, name VARCHAR, email VARCHAR, status VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, loyalty_tier VARCHAR)"
    )
    enforce(conn)  # must not raise - purely additive is not breaking
    report = check_all(conn)
    assert "loyalty_tier" in str(report)  # still surfaced as a warning though
