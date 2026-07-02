from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from pipeline.lake import create_lake_tables
from pipeline.warehouse import create_warehouse_tables
from source.models import create_source_tables


@pytest.fixture
def conn():
    connection = duckdb.connect(":memory:")
    create_source_tables(connection)
    create_lake_tables(connection)
    create_warehouse_tables(connection)
    return connection


@pytest.fixture
def reservation_row():
    return {
        "reservation_id": "r1",
        "customer_id": "c1",
        "event_id": "e1",
        "status": "held",
        "hold_expires_at": datetime(2026, 7, 1, 10, 15),
        "confirmed_at": None,
        "total_amount": "125.00",
        "currency": "USD",
        "created_at": datetime(2026, 7, 1, 10, 0),
        "updated_at": datetime(2026, 7, 1, 10, 0),
    }

