"""
Ingestion orchestrator.

Ties the layers together for one ingest cycle:

  1. schema-contract guard (fail closed on incompatible source change)
  2. read unprocessed records since the checkpoint
  3. append every change to the lake (idempotent)
  4. apply changes to the warehouse (current snapshot + SCD2 history)
  5. advance the checkpoint only after a successful cycle

Running it twice is a no-op (idempotent); restarting after a crash resumes from the
last checkpoint.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import duckdb

from ingestion.cdc import CDCCapture
from ingestion.checkpoint import create_checkpoint_table, get_checkpoint, set_checkpoint
from ingestion.schema_contract import assert_compatible
from lake.lake import append_to_lake, create_lake_table
from source.models import create_source_tables
from warehouse.warehouse import apply_cdc_records, create_warehouse_tables


@dataclass
class IngestionResult:
    applied: int
    lake_written: int
    checkpoint: int
    warnings: Dict[str, List[str]] = field(default_factory=dict)


def build_lakehouse(conn: duckdb.DuckDBPyConnection) -> None:
    """Create source, lake, warehouse, and checkpoint objects on one connection."""
    create_source_tables(conn)
    create_lake_table(conn)
    create_warehouse_tables(conn)
    create_checkpoint_table(conn)


def run_ingestion(
    conn: duckdb.DuckDBPyConnection,
    capture: CDCCapture,
    consumer: str = "default",
    enforce_schema: bool = True,
    observed_enums: Optional[Dict[str, Dict[str, Tuple[str, ...]]]] = None,
) -> IngestionResult:
    warnings: Dict[str, List[str]] = {}
    if enforce_schema:
        # Raises SchemaIncompatibilityError on a breaking change -> nothing is written.
        warnings = assert_compatible(conn, observed_enums=observed_enums)

    checkpoint = get_checkpoint(conn, consumer)
    records = capture.records_since(checkpoint)
    if not records:
        return IngestionResult(0, 0, checkpoint, warnings)

    lake_written = append_to_lake(conn, records)
    apply_cdc_records(conn, records)

    new_checkpoint = capture.latest_sequence
    set_checkpoint(conn, new_checkpoint, consumer)
    return IngestionResult(len(records), lake_written, new_checkpoint, warnings)


def _demo() -> None:
    from ingestion.seed import seed_capture
    from warehouse.warehouse import reconstruct_as_of, restore_from_lake

    conn = duckdb.connect(":memory:")
    build_lakehouse(conn)
    capture = seed_capture()

    result = run_ingestion(conn, capture)
    print(
        f"Ingested {result.applied} change(s); "
        f"{result.lake_written} written to lake; checkpoint={result.checkpoint}"
    )

    lake_total = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    print(f"Lake holds {lake_total} immutable change events.")

    current = conn.execute(
        "SELECT order_id, status, total FROM wh_orders WHERE NOT _deleted"
    ).fetchall()
    print(f"Current orders snapshot: {current}")

    early = reconstruct_as_of(conn, "orders", as_of_seq=9)
    print(f"Order o1 status at seq 9 (time travel): {early[0]['status']}")

    restore_from_lake(conn, target_seq=9)
    restored = conn.execute(
        "SELECT status FROM wh_orders WHERE order_id = 'o1'"
    ).fetchone()
    print(f"After restore_from_lake(seq=9), order o1 status: {restored[0]}")


if __name__ == "__main__":
    _demo()
