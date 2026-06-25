"""Shared pytest fixtures for the wallet/payments CDC pipeline tests."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.schema_detector import SchemaDetector
from source.models import create_source_tables
from source.seed import build_seed_rows


def _ts() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def conn() -> duckdb.DuckDBPyConnection:
    """Fresh in-memory DuckDB with source + lake + warehouse + checkpoints."""
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    bootstrap(c)
    return c


@pytest.fixture()
def capture() -> CDCCapture:
    """Empty CDC capture log."""
    return CDCCapture()


@pytest.fixture()
def detector(conn: duckdb.DuckDBPyConnection) -> SchemaDetector:
    return SchemaDetector(conn)


@pytest.fixture()
def seeded(
    conn: duckdb.DuckDBPyConnection,
    capture: CDCCapture,
    detector: SchemaDetector,
):
    """
    DuckDB connection pre-loaded with the canonical wallet seed
    (3 customers, 3 payment methods, 4 wallets, 3 transfers,
    5 transfer_events, 5 ledger_entries).

    Returns (conn, capture).
    """
    for row in build_seed_rows():
        capture.insert(row.table, row.pk, row.data)

    stats = run_tick(conn, capture, detector=detector)
    assert not stats["aborted"]
    return conn, capture
