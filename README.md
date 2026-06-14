# CDC Data Platform Assignment

## Overview

This project implements a CDC-based data platform for an e-commerce transactional system using:

* PySpark
* PostgreSQL (simulated locally using Spark datasets)
* Apache Airflow
* Parquet Data Lake
* SCD Type 2 Warehouse Modeling

The platform captures all source changes, stores immutable history in a data lake, maintains a near real-time analytical warehouse, detects incompatible schema changes, supports historical recovery, enforces validation parity, and exposes datasets through a catalog.

---

# Architecture

```text
Source System
(PostgreSQL)

      |
      v

Schema Validation
(Fail Fast)

      |
      v

CDC Capture
(WAL Based CDC)

      |
      v

Bronze Layer
Immutable CDC Events

      |
      v

Silver Layer
Latest Operational Snapshot

      |
      v

Warehouse Layer

  - dim_customer (SCD2)
  - dim_product (SCD2)

  - fact_orders
  - fact_order_items
  - fact_payment_attempts

      |
      v

Data Quality Checks

      |
      v

Catalog Publication
```

---

# Domain

The chosen domain is an e-commerce platform.

The model contains both strong and weak entities.

## Strong Entities

### Customers

Stores customer information.

### Products

Stores product catalog information.

### Orders

Represents customer purchases.

---

## Weak Entities

### Order Items

Dependent on Orders.

Represents individual products within an order.

### Payment Attempts

Dependent on Orders.

Represents payment gateway attempts and outcomes.

---

# Source Data Model

## customers

| Column          | Type              |
| --------------- | ----------------- |
| customer_id     | bigint            |
| email           | string            |
| first_name      | string            |
| last_name       | string            |
| phone           | string (nullable) |
| customer_status | enum              |
| created_at      | timestamp         |
| updated_at      | timestamp         |

Indexes:

* PK(customer_id)
* email

---

## products

| Column         | Type          |
| -------------- | ------------- |
| product_id     | bigint        |
| sku            | string        |
| product_name   | string        |
| category       | string        |
| unit_price     | decimal(18,2) |
| product_status | enum          |
| created_at     | timestamp     |
| updated_at     | timestamp     |

Indexes:

* PK(product_id)
* sku

---

## orders

| Column       | Type          |
| ------------ | ------------- |
| order_id     | bigint        |
| customer_id  | bigint        |
| order_status | enum          |
| total_amount | decimal(18,2) |
| created_at   | timestamp     |
| updated_at   | timestamp     |

Indexes:

* PK(order_id)
* customer_id

Foreign Keys:

* customer_id -> customers.customer_id

---

## order_items

| Column        | Type          |
| ------------- | ------------- |
| order_item_id | bigint        |
| order_id      | bigint        |
| product_id    | bigint        |
| quantity      | integer       |
| unit_price    | decimal(18,2) |
| line_amount   | decimal(18,2) |
| created_at    | timestamp     |

Indexes:

* PK(order_item_id)
* order_id
* product_id

Foreign Keys:

* order_id -> orders.order_id
* product_id -> products.product_id

---

## payment_attempts

| Column                 | Type          |
| ---------------------- | ------------- |
| payment_attempt_id     | bigint        |
| order_id               | bigint        |
| payment_status         | enum          |
| gateway_transaction_id | string        |
| amount                 | decimal(18,2) |
| attempt_timestamp      | timestamp     |

Indexes:

* PK(payment_attempt_id)
* order_id

Foreign Keys:

* order_id -> orders.order_id

---

# Entity Relationships

```text
Customer
   |
   | 1:M
   |
Orders
   |
   | 1:M
   |
Order Items
   |
   | M:1
   |
Products

Orders
   |
   | 1:M
   |
Payment Attempts
```

---

# Important Business Invariants

1. Customer IDs are unique.
2. Product IDs are unique.
3. Orders must reference a valid customer.
4. Order Items must reference valid orders and products.
5. Payment Attempts must reference valid orders.
6. Monetary amounts must be non-negative.
7. Sum of line items must equal order total.
8. Status transitions must follow allowed business workflow.

---

# CDC Pipeline

The solution is designed around WAL-based CDC.

## Production Architecture

```text
PostgreSQL
    |
    v
WAL
    |
    v
Debezium
    |
    v
Kafka
    |
    v
Spark
```

## Assignment Implementation

For local execution, CDC events are simulated using Python objects.

Each CDC event contains:

* sequence_number
* table_name
* operation
* primary_key
* before_image
* after_image
* event_timestamp

Supported operations:

* INSERT
* UPDATE
* DELETE

---

# Bronze Layer

Purpose:

Store immutable CDC history.

Dataset:

```text
data/bronze/cdc_events
```

Properties:

* Append-only
* Stores every change
* Supports replay
* Supports historical reconstruction

---

# Silver Layer

Purpose:

Create latest operational state.

Datasets:

```text
data/silver/customers
data/silver/products
data/silver/orders
data/silver/order_items
data/silver/payment_attempts
```

Properties:

* Latest record per business key
* CDC metadata removed
* Optimized for warehouse loading

---

# Warehouse Layer

## Dimensions

### dim_customer

SCD Type 2 dimension.

Tracks:

* customer status changes
* customer profile changes

Columns:

* effective_from
* effective_to
* is_current

---

### dim_product

SCD Type 2 dimension.

Tracks:

* product status changes
* product attribute changes

Columns:

* effective_from
* effective_to
* is_current

---

## Facts

### fact_orders

Order-level facts.

### fact_order_items

Line-level facts.

### fact_payment_attempts

Payment transaction facts.

---

# Historical Recovery and Time Travel

Historical recovery is supported through Bronze CDC history.

Process:

```text
Bronze CDC
      |
      v
Replay Until Timestamp
      |
      v
Rebuild Silver
      |
      v
Rebuild Warehouse
```

Capabilities:

* Point-in-time reconstruction
* Replay after failures
* Warehouse restoration
* Auditability

---

# Schema Change Detection

Schema contracts are maintained in:

```text
config/schema_contracts.json
config/enum_contracts.json
```

The pipeline validates source schemas before ingestion.

Detected breaking changes:

* Dropped column
* Renamed column
* Data type change
* Enum value changes
* Nullable changes
* Primary key changes
* Foreign key changes

Behavior:

```text
Schema Change Detected
        |
        v
Pipeline Fails
        |
        v
CDC Stops
        |
        v
Warehouse Not Updated
```

This prevents silent corruption.

---

# Validation Parity

## System Validations

### Primary Key Validation

* customer_id
* product_id
* order_id
* order_item_id
* payment_attempt_id

### Foreign Key Validation

* orders -> customers
* order_items -> orders
* order_items -> products
* payment_attempts -> orders

### Not Null Validation

Critical columns are validated.

---

## Business Validations

### Non-Negative Amounts

Validated:

* total_amount
* unit_price
* line_amount
* payment amount

### Order Total Validation

```text
SUM(order_items.line_amount)
=
orders.total_amount
```

### Status Transition Validation

Allowed:

```text
CREATED -> PAID
CREATED -> CANCELLED
PAID -> SHIPPED
SHIPPED -> DELIVERED
```

Rejected:

```text
DELIVERED -> CREATED
DELIVERED -> PAID
CANCELLED -> PAID
```

Validation failures stop the pipeline.

---

# Catalog Exposure

Catalog Metadata:

```text
catalog/catalog.json
```

Published Datasets:

## Bronze

* bronze.cdc_events

## Silver

* silver.customers
* silver.products
* silver.orders
* silver.order_items
* silver.payment_attempts

## Warehouse

* warehouse.dim_customer
* warehouse.dim_product
* warehouse.fact_orders
* warehouse.fact_order_items
* warehouse.fact_payment_attempts

Metadata Published:

* dataset name
* owner
* layer
* refresh frequency
* description
* storage path

---

# Airflow Workflow

```text
validate_schema
        |
        v
ingest_cdc
        |
        v
build_silver
        |
        v
build_warehouse
        |
        v
run_data_quality_checks
        |
        v
publish_catalog
```

Schedule:

Every 5 minutes.

---

# Project Structure

```text
submission/

├── airflow_dags/
│   └── ecommerce_cdc_pipeline.py

├── pipeline/
│   ├── bronze.py
│   ├── silver.py
│   ├── warehouse.py
│   ├── cdc.py
│   ├── schema_contracts.py
│   └── catalog.py

├── scripts/
│   ├── validate_schema.py
│   ├── ingest_cdc.py
│   ├── build_silver.py
│   ├── build_warehouse.py
│   ├── run_data_quality_checks.py
│   ├── status_transition_validation.py
│   ├── publish_catalog.py
│   └── rebuild_warehouse.py

├── source/
│   ├── create_tables.py
│   ├── seed_data.py
│   ├── models.py
│   └── postgres.py

├── catalog/
│   └── catalog.json

├── config/
│   ├── schema_contracts.json
│   └── enum_contracts.json

└── README.md
```

---

# Assumptions

1. Local execution uses Spark Parquet datasets instead of a live PostgreSQL instance.
2. WAL-based CDC is simulated for assignment scope.
3. Production deployment would use PostgreSQL WAL, Debezium, and Kafka.
4. Airflow orchestrates end-to-end pipeline execution.
5. Warehouse dimensions use SCD Type 2 history tracking.
