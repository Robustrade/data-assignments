# Local Validation Guide

This guide explains how to test the ticket reservations CDC pipeline locally. The goal is to prove platform behavior, including replay safety, schema failure handling, delete handling, historical reconstruction, and validation parity.

The local test harness uses DuckDB and Python to simulate the behavior of the production design:

- PostgreSQL source schema and schema contract
- Debezium-style CDC events
- append-only silver CDC event storage
- warehouse current and history tables
- catalog and quality validation scripts

## 1. Set Up the Local Environment

From the repository root:

```bash
cd submission/ticket-reservations
python -m pip install -r requirements.txt
```

Expected result:

- dependencies install successfully
- `pytest`, `duckdb`, and local pipeline modules are available

## 2. Run the Full Test Suite

```bash
pytest
```

This is the broadest local validation. It should pass all tests in `tests/`.

What this proves:

- CDC events follow the expected Debezium shape
- offsets and source LSNs are monotonic
- checkpoint replay boundaries work
- duplicate delivery is deduplicated by stable `event_id`
- invalid CDC operations are rejected
- incompatible schema changes fail closed
- additive nullable columns are allowed by contract policy
- append-only lake behavior preserves changes
- warehouse current state uses latest `source_lsn`
- deletes become soft deletes in the warehouse
- history supports point-in-time restore
- catalog entries are discoverable and documented
- quality checks detect reservation total mismatches

## 3. Validate CDC Behavior Directly

```bash
pytest tests/test_cdc.py
```

Important cases covered:

- create, update, and delete events have Debezium-style `before`, `after`, and `op` fields
- `kafka_offset` and `source_lsn` increase over time
- `events_after(checkpoint)` returns only events after the saved checkpoint
- duplicate event delivery produces the same deterministic `event_id`
- unsupported operations such as `upsert` are rejected

Why this matters:

The pipeline should be restartable and replayable. If a micro-batch fails, the same CDC files can be processed again without creating duplicate business changes.

## 4. Validate Schema Contract Safety

```bash
pytest tests/test_schema_contracts.py
python scripts/check_schema_contracts.py
```

Important cases covered:

- the source schema matches the registered contract
- dropping a required column fails ingestion
- changing a column type is detected as incompatible
- unknown tables are rejected
- nullable additive columns are accepted

Why this matters:

The pipeline fails closed on breaking schema changes instead of silently writing corrupted or ambiguous data.

## 5. Validate Lake and Warehouse Semantics

```bash
pytest tests/test_lake_warehouse.py
```

Important cases covered:

- the silver CDC table keeps every unique change event
- replaying the same events does not duplicate lake records
- warehouse current records use the latest `source_lsn`, even when events arrive out of order
- deletes are represented as soft deletes in current-state tables
- warehouse history can reconstruct prior state at a requested timestamp

Why this matters:

The lake is a durable event history, while the warehouse exposes current and point-in-time views derived from that history.

## 6. Validate Business Data Quality

```bash
pytest tests/test_data_quality.py
python scripts/run_data_quality_checks.py
```

Important cases covered:

- reservation totals must equal the sum of item unit prices and service fees
- mismatches are detected and returned as failures
- matching totals pass cleanly

Why this matters:

Warehouse validation mirrors source business rules instead of relying only on row counts or happy-path execution.

## 7. Validate Catalog Discoverability

```bash
pytest tests/test_catalog.py
python scripts/validate_catalog.py
```

Important cases covered:

- raw, silver, warehouse current, and warehouse history datasets are listed
- each dataset has owner, layer, cadence, keys, and description metadata

Why this matters:

The platform output is discoverable and reviewable, not just technically written somewhere.

## 8. Expected Negative-Test Behavior

These behaviors are intentionally tested because they prove the pipeline handles unsafe cases:

- dropped source column: rejected as an incompatible schema change
- source type change: reported as a schema violation
- unknown source table: rejected before ingestion
- duplicate CDC delivery: ignored by stable `event_id`
- stale out-of-order event: does not overwrite newer warehouse state
- delete event: retained in history and represented as deleted in current state
- reservation total mismatch: reported by data quality checks

Run these targeted checks to prove each negative or unsafe case:

```bash
pytest tests/test_schema_contracts.py::test_dropped_column_is_incompatible
pytest tests/test_schema_contracts.py::test_type_change_is_incompatible
pytest tests/test_schema_contracts.py::test_unknown_table_stops_ingestion
pytest tests/test_cdc.py::test_event_id_is_stable_for_duplicate_delivery
pytest tests/test_lake_warehouse.py::test_warehouse_current_uses_latest_lsn
pytest tests/test_lake_warehouse.py::test_delete_becomes_soft_delete_in_warehouse
pytest tests/test_data_quality.py::test_reservation_total_mismatch_is_detected
```

You can also run them together with:

```bash
pytest \
  tests/test_schema_contracts.py::test_dropped_column_is_incompatible \
  tests/test_schema_contracts.py::test_type_change_is_incompatible \
  tests/test_schema_contracts.py::test_unknown_table_stops_ingestion \
  tests/test_cdc.py::test_event_id_is_stable_for_duplicate_delivery \
  tests/test_lake_warehouse.py::test_warehouse_current_uses_latest_lsn \
  tests/test_lake_warehouse.py::test_delete_becomes_soft_delete_in_warehouse \
  tests/test_data_quality.py::test_reservation_total_mismatch_is_detected
```

## 9. Reviewer Checklist

Use this checklist when evaluating the local run:

- `pytest` passes
- schema contract script passes
- catalog validation script passes
- quality check script completes and reports the expected validation result
- tests include at least one failure/negative scenario for schema safety, duplicate replay, delete handling, and business-rule validation
- warehouse behavior is derived from CDC history rather than from a blind snapshot refresh
