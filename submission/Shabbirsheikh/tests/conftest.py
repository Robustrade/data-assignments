import duckdb
import pytest

from source.models import init_source_schema
from pipeline import warehouse
from tests.helpers import EventBuilder


@pytest.fixture
def source_conn():
    conn = duckdb.connect(":memory:")
    init_source_schema(conn)
    return conn


@pytest.fixture
def warehouse_conn():
    conn = duckdb.connect(":memory:")
    warehouse.create_warehouse_tables(conn)
    return conn


@pytest.fixture
def change_log():
    return EventBuilder()
