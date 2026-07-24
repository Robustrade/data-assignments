# CDC Lakehouse — E-commerce Orders

## Proposed Approach

My understanding is that the objective is to build a reliable CDC pipeline that keeps a lake and a warehouse synchronized with a transactional source, while handling replay, schema drift, and recovery scenarios.

Domain: an order-management slice of e-commerce — customers, orders, order_items, payments — chosen for a natural strong/weak entity split and a cross-table invariant (order total vs. line items) worth validating downstream.

### Initial Technology Stack

- Python
- DuckDB (source database, local — simulates the transactional OLTP system)
- Simulated CDC (DuckDB has no WAL/logical-replication slot to tail like Postgres, so change capture is emitted at the application layer as sequence-numbered insert/update/delete events, standing in for a real log)
- Kafka (event transport for change records, local broker)
- Spark Structured Streaming (consumes Kafka, writes lake + warehouse)
- Parquet (lake: append-only change history)
- Delta Lake (warehouse storage: latest-state snapshot via MERGE/upsert, with native time travel for restore — written by Spark, queried locally via DuckDB's `delta` extension so DuckDB remains the single query interface across source and warehouse)
- pytest (validation and correctness tests)
- Airflow (stretch goal: orchestration/scheduling wrapper once the core pipeline is correct)

### High-Level Flow

```
Source (DuckDB: customers, orders, order_items, payments)
  -> CDC capture (sequence-numbered insert/update/delete events, app-layer emitted)
  -> Kafka (change event topic)
  -> Spark Structured Streaming
       -> Lake (Parquet, append-only, full history)
       -> Warehouse (Delta Lake, latest-state via merge, time travel via Delta versioning)
```

Schema-contract checks run before ingestion to detect breaking source changes; the warehouse is rebuilt from the lake, so point-in-time reconstruction is possible either by replaying up to a given sequence/offset or via Delta's native `VERSION AS OF` / `TIMESTAMP AS OF`.

### Notes

This represents my initial approach before implementation. CDC capture itself is simulated at the application layer rather than a real WAL connector — DuckDB doesn't expose one — while Kafka, Spark, and Delta Lake are used for real as the transport, processing, and warehouse-storage layers. As development progresses, I may refine details such as schema-change detection, replay/duplicate handling, and warehouse time-travel modeling based on testing and practical considerations.
