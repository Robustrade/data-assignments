# CDC Lakehouse Design

## 1. Domain

E-commerce Order Management System.

The system manages customers, products, orders, order items, and payments.

The transactional source system is represented using SQLite for local reproducibility.

---

## 2. Source Schema

### Tables

1. customers
2. products
3. orders
4. order_items
5. payments

### Strong Entities

- customers
- products
- orders
- payments

### Weak Entity

- order_items

An order item depends on an order and represents a product purchased as part of that order.

---

## 3. Relationships

customers 1:N orders

orders 1:N order_items

products 1:N order_items

orders 1:N payments

---

## 4. Source Validations

The source system will enforce:

- primary key uniqueness
- foreign key relationships
- required fields using NOT NULL
- non-negative product prices
- non-negative order totals
- valid order statuses
- valid payment statuses
- order item quantity greater than zero
- payment amount greater than zero

---

## 5. CDC Strategy

For this local assignment, CDC will be simulated using a change log.

Every INSERT, UPDATE and DELETE operation will generate a CDC event containing:

- event_id
- table_name
- operation
- primary_key
- before state
- after state
- captured_at

The durable change log assigns a monotonically increasing sequence_number
to each unique event to provide deterministic ordering.

---

## 6. Duplicate Handling

Each CDC event will have a unique event_id.

The ingestion process will maintain processed event identifiers.

If an event is replayed, the event will not be applied twice.

---

## 7. Replay and Restart

CDC processing will maintain a checkpoint containing the latest successfully processed sequence number.

After a failure or restart, processing resumes from the checkpoint.

Events before the checkpoint are skipped safely.

---

## 8. Lake Design

The lake stores every CDC event in append-oriented files.

Each event contains:

- source table
- operation
- primary key
- before state where applicable
- after state where applicable
- captured timestamp
- sequence number assigned by the change log

The lake is the historical source of CDC events.

No CDC event is overwritten.

---

## 9. Warehouse Design

DuckDB will be used as the local warehouse.

The warehouse will maintain:

- current customers
- current products
- current orders
- current order items
- current payments

DELETE events will remove records from the current snapshot.

UPDATE events will replace the previous state.

INSERT events will create new records.

---

## 10. Historical Recovery

The lake contains the complete CDC history.

A historical state can be reconstructed by replaying CDC events up to a selected sequence number or timestamp.

The warehouse can therefore be restored by rebuilding the current-state tables from the historical event stream.

---

## 11. Schema Change Safety

The pipeline will maintain an expected schema contract.

Before processing CDC events, the current source schema will be compared with the expected schema.

The following changes are considered incompatible:

- dropped columns
- renamed columns
- changed data types
- changed nullable requirements
- changed primary key definitions
- changed foreign key definitions
- incompatible status/domain changes

If an incompatible change is detected:

1. CDC ingestion stops.
2. A clear error is emitted.
3. No new lake or warehouse data is processed.
4. The expected schema must be reviewed before ingestion resumes.

---

## 12. Validation Parity

Source validations will be represented downstream.

The warehouse validation layer will check:

- primary key uniqueness
- foreign key integrity
- required fields
- valid statuses
- non-negative monetary values
- positive quantities
- payment/order consistency

Validation failures will cause the data-quality check to fail.

---

## 13. Catalog

A lightweight JSON catalog will publish metadata for:

### Lake datasets

- CDC event history

### Warehouse datasets

- customers
- products
- orders
- order_items
- payments

Catalog metadata will include:

- dataset name
- layer
- description
- owner
- intended users
- schema
- update cadence

---

## 14. Reliability

The pipeline will explicitly handle:

- duplicate events
- retries
- restart from checkpoint
- deletes
- late events
- incompatible schema changes
- historical reconstruction

---

## 15. Testing Strategy

Tests will cover:

### Source Model

- primary keys
- foreign keys
- constraints

### CDC

- INSERT
- UPDATE
- DELETE
- duplicate events
- replay
- restart

### Schema Safety

- incompatible schema detection
- ingestion stop

### Warehouse

- latest snapshot
- validation parity
- historical reconstruction

### Catalog

- lake metadata exists
- warehouse metadata exists

---

## 16. Technology

- Python
- SQLite
- DuckDB
- SQL
- pytest
- JSON/JSONL

---

## 17. Assumptions

CDC is simulated locally rather than using a production database WAL/log-based CDC mechanism.

In production, the CDC source could be PostgreSQL WAL, Debezium, Kafka or another database-log based solution.

The local implementation focuses on demonstrating correctness, replayability, schema safety and historical recovery.