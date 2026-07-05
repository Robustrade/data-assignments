# CDC Lakehouse Reliability — Global Trade & Cargo Logistics

This directory contains the Change Data Capture (CDC) Lakehouse synchronization pipeline tailored for **ROBUSTRADE's** international trading, supply chain, and global cargo logistics operations.

---

## 1. Architecture Overview

The system is built on a clean separation of concerns, separating transaction systems, raw immutable history, analytical querying, and data governance:

```
[ OLTP Source DB ]  ---(Writes WAL)--->  [ CDC Capture Agent ]
                                                 │
                                                 ▼
[ OLAP Warehouse ]  <---(Upsert/Merge)---  [ Raw S3 Lake ]
 (Silver Target)                            (Bronze History)
```

1. **Source Layer (OLTP):** A relational, highly-normalized database representing operational trades, shipping containers, customs clearances, and wire/credit payments.
2. **Ingestion & CDC Layer:** Continuously reads transactional log changes (Write-Ahead Logs/WAL) and publishes ordered events carrying monotonic sequence numbers.
3. **Lake Layer (Bronze):** An append-only, immutable history log preserving every single state change as JSON records.
4. **Warehouse Layer (Silver):** A consolidated, analytical current-state snapshot of the operational data. Supports Point-in-Time recovery (Time Travel) by replaying the Lake logs.
5. **Quality & Validation Layer:** Enforces both database integrity constraints (PK/FK/NOT NULL) and complex trade business rules (e.g. overpayment audits, customs-blocked deliveries).
6. **Catalog Layer:** Exposes schema metadata, descriptions, update cadences, and consumer permissions in a centralized registry.

---

## 2. Source Schema & Domain Design

The domain models international commodity trade agreements and container cargo shipping logistics.

### Entity Relationship Diagram
```
  [ trade_partners ] (Seller/Buyer)
         │
         ├── (1-to-many) ──┐
         ▼                 ▼
  [ trade_contracts ] ─────┼───── (1-to-many) ──┐
         │                 │                    │
         ├── (1-to-many) ──┼──┐                 │
         ▼                 ▼  ▼                 ▼
  [ shipment_cargo ]   [ customs_clearances ]   [ settlement_transactions ]
```

### Table Definitions

* **`trade_partners`** (Strong Entity): Profiles buyers and sellers (e.g. Exporters in Indonesia, Importers in India).
  * *Attributes:* `partner_id` (PK), `company_name`, `country`, `partner_type` (`buyer` | `seller`), `compliance_status` (`active` | `suspended` | `under_review`).
* **`trade_contracts`** (Strong Entity): Financial agreements detailing purchase pricing and weight.
  * *Attributes:* `contract_id` (PK), `seller_id` (FK), `buyer_id` (FK), `commodity_type`, `weight_metric_tons`, `price_per_ton_usd`, `total_value_usd`, `status` (`pending` | `active` | `completed` | `cancelled`).
* **`shipment_cargo`** (Weak Entity): Physical ocean containers loaded at port of origin and discharged at port of destination.
  * *Attributes:* `cargo_id` (PK), `contract_id` (FK), `carrier_name`, `container_number`, `port_of_loading`, `port_of_discharge`, `estimated_delivery`, `actual_delivery` (Nullable).
* **`customs_clearances`** (Weak Entity): Regulatory tariff approvals and duty declarations.
  * *Attributes:* `clearance_id` (PK), `contract_id` (FK), `clearing_country`, `duty_fee_usd`, `status` (`pending` | `approved` | `held_in_customs` | `rejected`), `cleared_at` (Nullable).
* **`settlement_transactions`** (Weak Entity): Financial wire transfers and letters of credit settling contract dues.
  * *Attributes:* `transaction_id` (PK), `contract_id` (FK), `payment_amount_usd`, `settlement_type` (`wire_transfer` | `letter_of_credit`), `status` (`pending` | `settled` | `failed`), `settled_at` (Nullable).

---

## 3. CDC Strategy & Assumptions

* **Log Sequence Numbers (LSN):** Every change operation generates a `CDCRecord` with a monotonically increasing `sequence` number. This acts as our LSN for checkpoint restarts.
* **Duplicate & Replay Safety:** Downstream warehouse merges enforce the rule `incoming_record.sequence > current_warehouse_row._cdc_seq`. Any duplicate or out-of-order event arriving due to network retry is safely discarded, guaranteeing idempotency.
* **Delete Propagation:** Hard deletes in the source are captured and translated to soft-deletes in the Warehouse (`_deleted = true`, preserving the row and its historical LSN for auditing).

---

## 4. Schema Change Safety & Evolution Policy

Our pipeline operates under a **"Fail-Closed / Stop-the-Line"** schema evolution policy. 

* **Breaking Changes:** Dropping a column, changing a data type, or altering a key constraint are classified as incompatible changes.
* **Detection Mechanism:** Before pulling data, our contract scanner inspects the operational source database against the schema contract registry.
* **Aborting Ingestion:** If a mismatch is found, the script throws an alert and terminates with exit code `1`. The pipeline refuses to capture or write any events, protecting downstream tables from corruption.

---

## 5. Historical Recovery & Time Travel

Because the Lake retains an immutable record of every mutation, we can recover database states at any past point in time:
1. The function `reconstruct_warehouse_at(conn, target_lsn)` drops current warehouse tables and recreates empty schemas.
2. It queries `lake_cdc_events` for events where `sequence <= target_lsn`.
3. It replays these events sequentially using our merge logic.
4. The warehouse is restored to its exact historical state at that sequence number.

---

## 6. Validation Parity

To protect downstream analysts, we run the following validations directly in the Warehouse:

### Relational Checks
* **Uniqueness:** Asserts `primary_key` is unique across active rows.
* **Referential Integrity:** Asserts that child keys (e.g. `contract_id`) resolve to active parent records in `wh_trade_contracts`.
* **Null Check:** Asserts that required attributes contain no NULLs in active rows.

### Business Rules
* **Math Drift:** Asserts `total_value_usd == weight_metric_tons * price_per_ton_usd`.
* **Overpayment Block:** Sum of settled payments for a contract must be less than or equal to `total_value_usd`.
* **Customs Delivery Block:** A container shipment cannot be physically delivered (`actual_delivery` is set) if its corresponding customs status is `rejected` or `held_in_customs`.

---

## 7. Setup & Execution Instructions

### A. Environment Configuration
Create a virtual environment and install the required dependencies (DuckDB and Pytest):
```bash
# Create virtual environment
python -m venv .venv

# Activate (PowerShell)
.venv\Scripts\Activate.ps1
# Activate (Windows CMD)
.venv\Scripts\activate.bat

# Install dependencies
pip install -r submission/harshal-logistics/requirements.txt
```

### B. Running Automated Tests
Run the pytest suite containing **29 comprehensive test assertions** checking CDC monotonicity, schema contracts, business quality limits, and point-in-time recovery:
```bash
pytest -v submission/harshal-logistics/tests
```

### C. Running Verification Scripts Manually
Run the pipeline and data governance scripts directly to verify their operational exit status:

1. **Schema Contract Compliance:**
   ```bash
   python submission/harshal-logistics/scripts/check_schema_contracts.py
   ```
2. **Data Quality & Business Rules Auditing:**
   ```bash
   python submission/harshal-logistics/scripts/run_data_quality_checks.py
   ```
3. **Dataset Catalog Validation:**
   ```bash
   python submission/harshal-logistics/scripts/validate_catalog.py
   ```
