"""
Pytest configuration and fixtures for CDC Lakehouse Reliability tests.
"""

from datetime import datetime, timezone

import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.lake import create_lake_table
from pipeline.warehouse import create_warehouse_tables
from source.models import create_source_tables


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with source, lake, and warehouse tables initialized."""
    db_conn = duckdb.connect(":memory:")
    create_source_tables(db_conn)
    create_lake_table(db_conn)
    create_warehouse_tables(db_conn)
    return db_conn


@pytest.fixture
def cdc_stream() -> CDCCapture:
    """Fresh CDC capture instance."""
    return CDCCapture()


@pytest.fixture
def sample_timestamps() -> dict[str, datetime]:
    """Standardized UTC timestamps for test fixtures."""
    now = datetime.now(timezone.utc)
    return {"now": now}
