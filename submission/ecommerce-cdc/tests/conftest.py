"""Shared fixtures for the e-commerce CDC lakehouse tests."""

import duckdb
import pytest

from ingestion.cdc import CDCCapture
from ingestion.pipeline import build_lakehouse, run_ingestion
from ingestion.seed import seed_capture


@pytest.fixture()
def conn() -> duckdb.DuckDBPyConnection:
    """Fresh in-memory lakehouse: source + lake + warehouse + checkpoint."""
    c = duckdb.connect(":memory:")
    build_lakehouse(c)
    return c


@pytest.fixture()
def capture() -> CDCCapture:
    """The deterministic representative scenario (not yet ingested)."""
    return seed_capture()


@pytest.fixture()
def ingested(conn, capture):
    """Lakehouse after one full ingest cycle of the seed scenario."""
    run_ingestion(conn, capture)
    return conn
