# CDC Lakehouse Assignment - E-Commerce Order Management System

## Domain Choice

The source system models e-commerce order processing because it gives a realistic mix of independent entities, lifecycle-dependent entities, monetary values, timestamps, status domains, and cross-table business rules.

## Source Schema

Strong entities:
- `customers`
- `products`
- `orders`

Weak entities:
- `order_items`
- `inventory_logs`
- `shipments`
- `returns`

Key invariants:
- `order.total_amount = SUM(order_items.line_total)`
- `product.stock_qty >= 0`
- `order_item.line_total = quantity * unit_price`
- `shipment.delivered_at >= shipment.shipped_at`
- `return.refund_amount <= order_item.line_total`
- Status fields must stay inside documented enum domains.

## CDC Strategy

`CDCCapture` simulates WAL-style CDC. Every insert, update, and delete produces a `CDCRecord` with:
- `sequence`
- `operation`
- `table`
- `primary_key`
- `data`
- `captured_at`

In production this would map to Debezium/Postgres WAL or a similar database log reader. The `sequence` acts like a local LSN/Kafka offset and supports checkpoint-based replay.

## Lake Model

`lake_cdc_events` is the immutable event log. It stores every CDC event with sequence, operation, table name, primary key, JSON row data, and capture timestamp.

The lake is the historical source of truth. Retries are made idempotent by skipping events whose sequence already exists in the lake.

## Warehouse Model

The warehouse contains current-state tables only:
- `wh_customers`
- `wh_products`
- `wh_orders`
- `wh_order_items`
- `wh_inventory_logs`
- `wh_shipments`
- `wh_returns`

Each row includes:
- `_cdc_seq`: latest CDC sequence applied to the row
- `_deleted`: soft-delete flag for source deletes

Historical recovery does not use SCD2 tables. Instead, the warehouse can be restored by clearing current-state tables and replaying `lake_cdc_events` up to a target sequence. Point-in-time queries replay lake events up to a target timestamp and fold them into row state.

## Schema Change Safety

`SCHEMA_CONTRACT` documents the expected source columns. `scripts/check_schema_contracts.py` validates the source schema before ingestion and fails closed if required columns are missing. This prevents silent ingestion under broken downstream assumptions.

Production hardening would extend this contract to compare data types, nullability, check domains, and key constraints.

## Validation Parity

Source validations are represented through DuckDB constraints where practical and mirrored downstream with `scripts/run_data_quality_checks.py`.

The quality checks cover:
- Primary-key uniqueness
- Referential integrity
- Required fields
- Enum domains
- Positive monetary amounts
- Line-item totals
- Order totals
- Shipment date ordering
- Return refund limits

## Catalog Exposure

`catalog/catalog.json` publishes the lake dataset and all warehouse datasets with layer, owner, consumers, update cadence, schema metadata, descriptions, lineage, and validation rules.

## Tradeoffs

This solution uses local DuckDB and simulated CDC to keep the assignment runnable in a small environment. Production equivalents would use a relational source, WAL-based CDC, Kafka or a managed stream, object storage with table formats such as Delta/Iceberg/Hudi, and a warehouse merge job.

The restore model deliberately uses lake replay instead of SCD2 to keep one authoritative history path and avoid drift between separate history tables and the raw CDC log.
