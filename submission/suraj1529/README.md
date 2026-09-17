# CDC Lakehouse Reliability Assignment

A small, reproducible CDC pipeline for an e-commerce transactional source.

## Architecture

```text
PostgreSQL-like source simulation
        |
        v
  CDC event log (append-only)
        |
        +------------------> Lake: raw CDC JSONL
        |
        +------------------> Warehouse: current-state tables
                                  |
                                  +--> SCD2 history
                                  +--> validation results
        |
        +--> schema contract / safe-stop
        |
        +--> checkpoint for replay/restart
```

The implementation is intentionally local and dependency-light. It uses Python and SQLite to simulate a relational source and warehouse, while the lake is represented by immutable JSONL files. In production, the source CDC log could be PostgreSQL WAL/Debezium, the lake could be object storage + Delta/Iceberg, and the warehouse could be Snowflake/Databricks SQL.

## Domain

The model contains five tables:

- `customers` - strong entity
- `products` - strong entity
- `orders` - strong entity
- `order_items` - weak entity identified by `(order_id, line_number)`
- `payments` - weak entity owned by an order

Relationships:

`customers -> orders -> order_items -> products` and `orders -> payments`.

The schema includes decimal/currency fields, timestamps, status/enum-like fields, nullable attributes, foreign keys and indexes.

## Requirements covered

- Insert/update/delete CDC capture
- Durable append-only lake history
- Warehouse latest-state projection
- SCD2 historical reconstruction
- Checkpointed replay/restart
- Idempotent duplicate-event handling
- Out-of-order protection using source sequence numbers
- Schema contract validation and safe stop on breaking changes
- Source/warehouse validation parity
- Lightweight catalog metadata
- Automated tests for happy and non-happy paths

## Run

Python 3.10+ is recommended.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt
python -m cdc_pipeline.demo
pytest -q
```

The demo creates `runtime/` and shows the source, CDC, lake, warehouse and recovery flow.

## Repository layout

```text
sql/
  source_schema.sql
src/cdc_pipeline/
  __init__.py
  config.py
  source.py
  cdc.py
  lake.py
  warehouse.py
  schema_guard.py
  validation.py
  catalog.py
  pipeline.py
  demo.py
tests/
  test_pipeline.py
catalog/
  catalog.json
docs/
  architecture.md
```

## CDC contract

Every event has:

- `event_id` - deterministic unique event identifier
- `table_name`
- `operation` - INSERT/UPDATE/DELETE
- `primary_key`
- `row_data`
- `changed_at`
- `source_lsn` - monotonically increasing source sequence
- `schema_version`

The lake is append-only. Warehouse application is idempotent on `event_id` and rejects stale source sequence numbers.

## Safe-stop behavior

Before applying a batch, the pipeline compares the observed source schema to the approved contract. Breaking changes include dropped/renamed columns, incompatible types, nullable changes, enum-domain changes and key changes.

On a breaking change:

1. the batch is not applied;
2. a `schema_guard` error is emitted;
3. the checkpoint does not advance;
4. the operator can correct the contract/source and replay safely.

For the assignment's local scope, the source schema is inspected through SQLite metadata. In production this would be a source schema registry/contract check against CDC metadata.

## Time travel / restore

The warehouse stores SCD2 history with `valid_from`, `valid_to`, `is_current` and source sequence. A point-in-time query reconstructs the state as of a timestamp. A restore can be performed by rebuilding current tables from the SCD2 rows valid at the requested timestamp.

The lake remains the immutable source of change history; the SCD2 warehouse is an operationally convenient historical projection.

## Validation parity

Source constraints include primary keys, foreign keys, NOT NULL and CHECK constraints. Warehouse checks reproduce important rules:

- unique primary keys
- foreign-key relationships
- valid statuses
- non-negative monetary values
- order total equals order-item total
- payment date is not before order creation

Failures are written to `runtime/validation_results.jsonl` and cause the validation command to fail.

## AI usage

AI was used to accelerate scaffolding and review ideas. The implementation, assumptions, tests and final behavior should be personally reviewed and validated before submission.
