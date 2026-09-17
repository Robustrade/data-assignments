# Architecture and Tradeoffs

## Why an e-commerce model?

Orders and line items naturally demonstrate strong/weak entities, foreign keys, monetary values, status domains, deletes and aggregate validation.

## Reliability decisions

### Lake first
CDC events are appended to the lake before the warehouse checkpoint advances. If warehouse processing fails, the same events remain replayable.

### Idempotency
`event_id` is persisted in `applied_events`. Replaying the same event does not create a duplicate warehouse mutation.

### Ordering
`source_lsn` represents the source log sequence. A stale event is ignored if its sequence is older than the current row version.

### Deletes
Deletes are stored in the lake and represented in warehouse history as a `DELETE` SCD2 row; the current-state table removes the row.

### Schema safety
Breaking schema changes fail before any batch is applied. Additive fields are detected but intentionally do not auto-migrate the curated model.

### Recovery
A checkpoint identifies the highest successfully processed source sequence. Since the lake is durable, the pipeline can replay from the checkpoint or rebuild the warehouse from lake history.

## Production analogue

- Source CDC: PostgreSQL WAL + Debezium/Kafka or native CDC
- Lake: object storage + Delta Lake/Iceberg
- Warehouse: Snowflake/Databricks SQL
- Orchestration: Airflow/Dagster
- Catalog: Unity Catalog/DataHub/OpenMetadata
- Observability: metrics, alerts and dead-letter/error streams

The local implementation is a simulation rather than a claim of production-grade CDC infrastructure.
