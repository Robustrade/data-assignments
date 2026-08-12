"""
Tests for Data Lake append-only storage and idempotency deduplication.
"""

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake


def test_lake_append_and_retention(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify CDC records are written to lake_cdc_events with append-only log semantics."""
    cdc = CDCCapture()
    cdc.insert("customers", "c100", {"name": "Charlie"})
    cdc.update("customers", "c100", {"name": "Charlie Brown"})

    written = append_to_lake(conn, cdc.log)
    assert written == 2

    count = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert count == 2

    rows = conn.execute(
        "SELECT sequence, operation, table_name, primary_key FROM lake_cdc_events ORDER BY sequence"
    ).fetchall()
    assert rows[0] == (1, "insert", "customers", "c100")
    assert rows[1] == (2, "update", "customers", "c100")


def test_lake_idempotent_deduplication(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify re-processing the same sequence events does not create duplicate lake entries."""
    cdc = CDCCapture()
    cdc.insert("wallets", "w10", {"balance": 50.0})

    written_first = append_to_lake(conn, cdc.log)
    assert written_first == 1

    # Re-process identical batch
    written_second = append_to_lake(conn, cdc.log)
    assert written_second == 0  # Deduplicated

    total_count = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert total_count == 1
