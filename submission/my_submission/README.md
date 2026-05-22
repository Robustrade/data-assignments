# E-Commerce CDC Lakehouse Assignment

A change data capture (CDC) pipeline for an e-commerce order management system, demonstrating:
- **Source schema** with 7 tables (3 strong, 4 weak entities)
- **CDC capture** with simulated WAL-based change tracking
- **Lake layer** with immutable append-only event log
- **Warehouse layer** with current-state snapshots
- **Validation parity** between source and warehouse
- **Schema change detection** with safe-stop behavior
- **Point-in-time recovery** via lake replay
---

## Project Structure

```
my-submission/
├── source/
│   ├── __init__.py
│   └── models.py              # 7 tables + schema contracts + indexes
├── pipeline/
│   ├── __init__.py
│   ├── cdc.py                 # CDC capture simulator (sequences, operations)
│   ├── lake.py                # Immutable append-only lake
│   └── warehouse.py           # Current-state warehouse with upserts
├── scripts/
│   ├── __init__.py
│   ├── check_schema_contracts.py   # Schema validation
│   ├── run_data_quality_checks.py  # Business rule validation
│   └── validate_catalog.py         # Catalog integrity
sample
│   ├── sample_data.py              # Sample data generator
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # pytest fixtures
│   ├── test_schema_contracts.py    # Schema contract 
│   ├── test_cdc.py                 # CDC correctness tests
│   ├── test_warehouse.py           # Warehouse application tests
│   └── test_point_in_time.py       # Historical recovery tests
├── catalog/
│   └── catalog.json                # Dataset metadata
└── requirements.txt                # Dependencies
├── .gitignore                      # git ignore file
├── main.py                         # main orechestrator
├── core.py                         # core logic of orechestrator
├── APPROACH.md                     # Design decisions
├── IMPLEMENTATION_PLAN.md          # Design decisions


```

---

## Requirements Coverage

### ✅ Requirement 1: Source Data Model
- **7 tables**: customers, products, orders, order_items, inventory_logs, shipments, returns
- **Strong entities**: customers, products, orders (independent)
- **Weak entities**: order_items, inventory_logs, shipments, returns (lifecycle-dependent)
- **Relationships**: Foreign keys on customer_id, product_id, order_id
- **Indexes**: 13 indexes on FK and search columns
- **Data types**:
  - Currency: `DECIMAL(18,2)` for prices, amounts
  - Timestamps: `TIMESTAMP` for created_at, updated_at
  - Enums: `VARCHAR CHECK (...)` for status fields
  - Identifiers: `VARCHAR PRIMARY KEY`
  - Nullable: order_items has no updated_at, shipments have optional carrier

### ✅ Requirement 2: CDC Pipeline
- **Capture**: `CDCCapture` class simulates WAL-based change capture
- **Operations**: insert, update, delete
- **Sequences**: Monotonic sequence numbers for replay
- **Lake**: Immutable append-only storage via `append_to_lake()`
- **Warehouse**: Upsert logic via `apply_cdc_records()`
- **Replay**: `records_since(offset)` enables checkpoint-based recovery

### ✅ Requirement 3: Schema Change Detection
- **Schema contract**: Explicit column lists in `SCHEMA_CONTRACT`
- **Validation**: `check_schema_contracts.py` before CDC runs
- **Safe stop**: If columns missing → exit 1, ingestion stops
- **No silent failures**: Clear error messages

### ✅ Requirement 4: Historical Recovery & Time Travel
- **Lake immutability**: All changes preserved forever
- **Replay logic**: Filter lake by `captured_at <= target_date`, apply in sequence
- **Point-in-time**: Tests in `test_point_in_time.py` demonstrate recovery
- **Checkpoint mechanism**: `records_since(offset)` for resume-safe replay

### ✅ Requirement 5: Validation Parity
- **System validations**: PK, FK, NOT NULL, CHECK constraints
- **Business rules**:
  - `order.total_amount = SUM(order_items.line_total)`
  - `product.stock_qty >= 0`
  - `order_item.line_total = quantity × unit_price`
  - `shipment.delivered_at >= shipment.shipped_at`
  - `return.refund_amount <= order_item.line_total`
  - Status transitions (state machines)
- **Testing**: `run_data_quality_checks.py` validates all rules

### ✅ Requirement 6: Catalog Exposure
- **catalog.json**: 8 datasets (1 lake + 7 warehouse)
- **Metadata**: name, layer, description, owner, consumers, update_cadence
- **Schemas**: Column definitions for each table
- **Lineage**: Depends_on, used_by relationships
- **Validation rules**: Business invariants documented

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Database | DuckDB (in-memory OLAP) |
| Language | Python 3.9+ |
| Testing | pytest |
| Serialization | JSON (for CDC data) |
| Schema validation | Python dataclasses + custom checks |

---

## Installation & Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run schema contract validation

```bash
python scripts/check_schema_contracts.py
```

Expected output:
```
✅ All schema contracts passed (7 tables checked)
   Safe to proceed with CDC ingestion
```
### 3. Validate catalog

```bash
python scripts/validate_catalog.py
```

### 3. Run the END to END pipeline and orchestrate

```bash
python main.py
```

### 4. Run tests

```bash
pytest tests/ -v
```

Expected tests:
- `test_schema_contracts.py` — 4 tests (contract definition, validation, detection)
- `test_cdc.py` — 8 tests (insert, update, delete, sequences, replay)
- `test_warehouse.py` — 8 tests (upserts, soft deletes, FK integrity, decimals, timestamps)
- `test_point_in_time.py` — 5 tests (lake immutability, checkpoint replay, time travel)



---

## Example Usage

```python
import duckdb
from source.models import create_source_tables, create_indexes
from pipeline.cdc import CDCCapture
from pipeline.lake import create_lake_table, append_to_lake
from pipeline.warehouse import create_warehouse_tables, apply_cdc_records

# Setup
conn = duckdb.connect(":memory:")
create_source_tables(conn)
create_indexes(conn)
create_lake_table(conn)
create_warehouse_tables(conn)

# Simulate CDC
cdc = CDCCapture()

# Insert customer
customer = {
    "customer_id": "C001",
    "name": "Alice",
    "email": "alice@example.com",
    "country": "USA",
    "status": "active",
    "created_at": datetime.now(timezone.utc),
    "updated_at": datetime.now(timezone.utc),
}
record = cdc.insert("customers", "C001", customer)

# Write to lake
append_to_lake(conn, cdc.log)

# Update warehouse
apply_cdc_records(conn, cdc.log)

# Query warehouse
result = conn.execute("SELECT * FROM wh_customers WHERE _deleted = false").fetchall()
print(f"Current customers: {result}")

# Checkpoint for replay
checkpoint = cdc.latest_sequence
print(f"Checkpoint: {checkpoint}")

# Simulate recovery: get only new changes since checkpoint
new_changes = cdc.records_since(checkpoint)
print(f"Changes since checkpoint: {new_changes}")
```

---