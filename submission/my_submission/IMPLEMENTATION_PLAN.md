# CDC Lakehouse Implementation Plan

## Architecture

The solution is organized around a simple event-sourced lakehouse flow:

`source tables -> CDC records -> immutable lake -> current-state warehouse -> catalog`

## Layers

Source:
- Seven relational e-commerce tables.
- Strong and weak entities.
- Primary keys, foreign keys, check constraints, decimal values, timestamps, status domains, and indexes.

CDC:
- `CDCCapture` records insert, update, and delete events.
- Each event has a monotonic sequence for ordering and replay.
- `records_since(offset)` supports checkpoint-style restart.

Lake:
- `lake_cdc_events` is append-oriented and stores every change.
- The lake is the authoritative historical record.
- Duplicate retry appends are skipped by sequence.

Warehouse:
- Warehouse tables expose latest/current state only.
- `_cdc_seq` records the latest event applied to each row.
- `_deleted` preserves source deletes as soft deletes.
- Restore clears current-state tables and replays lake events up to a target sequence.
- Point-in-time reads replay lake events up to a target timestamp.

Validation:
- Source constraints are mirrored by warehouse/data-quality checks.
- Checks cover keys, referential integrity, enum domains, monetary rules, totals, and date-order rules.

Catalog:
- `catalog/catalog.json` publishes lake and warehouse datasets with schema, purpose, ownership, consumers, update cadence, lineage, and validation rules.

## Reliability Behaviors

- Inserts, updates, and deletes are captured.
- Lake retains the full CDC history.
- Warehouse reflects current state after applying CDC records.
- Replays after checkpoint use CDC sequence ordering.
- Duplicate lake writes from retries are skipped.
- Schema contract failures stop ingestion before downstream application.
- Historical recovery is performed from the lake, not from separate SCD2 tables.

## Known Production Extensions

- Replace simulated CDC with Debezium/Postgres WAL or equivalent.
- Use durable cloud/object storage with a table format such as Delta, Iceberg, or Hudi.
- Store CDC checkpoint state outside the process.
- Extend schema contracts to validate types, nullability, keys, and enum domains.
- Add monitoring, alerting, PII controls, and lineage tooling.
