"""
conftest.py — pytest fixtures for CDC/warehouse testing.

Provides:
- test database connection
- source tables setup
- sample data fixtures
"""

import pytest
import duckdb
from datetime import datetime, timezone

from source.models import create_source_tables, create_indexes
from pipeline.cdc import CDCCapture
from pipeline.lake import create_lake_table
from pipeline.warehouse import create_warehouse_tables


@pytest.fixture
def conn():
    """Provide a fresh in-memory DuckDB connection."""
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def setup_db(conn):
    """Create all source, lake, and warehouse tables."""
    create_source_tables(conn)
    create_lake_table(conn)
    create_indexes(conn)
    create_warehouse_tables(conn)
    return conn


@pytest.fixture
def cdc_capture():
    """Provide a fresh CDC capture instance."""
    return CDCCapture()


@pytest.fixture
def sample_customer():
    """Sample customer data."""
    return {
        "customer_id": "CUST001",
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "country": "USA",
        "status": "active",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_product():
    """Sample product data."""
    return {
        "product_id": "PROD001",
        "sku": "SKU-001",
        "name": "Wireless Mouse",
        "category": "Electronics",
        "price": 29.99,
        "stock_qty": 100,
        "status": "active",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_order(sample_customer):
    """Sample order data."""
    return {
        "order_id": "ORD001",
        "customer_id": sample_customer["customer_id"],
        "order_date": datetime(2026, 5, 1).date(),
        "status": "pending",
        "total_amount": 59.98,
        "currency": "USD",
        "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_order_item(sample_order, sample_product):
    """Sample order item data."""
    return {
        "order_item_id": "ITEM001",
        "order_id": sample_order["order_id"],
        "product_id": sample_product["product_id"],
        "quantity": 2,
        "unit_price": 29.99,
        "line_total": 59.98,
        "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
    }
