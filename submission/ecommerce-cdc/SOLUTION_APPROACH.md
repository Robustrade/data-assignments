# Solution Approach — E-commerce CDC Lakehouse

A change-data-capture (CDC) pipeline that keeps two derived stores in sync with an
e-commerce source system:

- a **lake** — append-only, immutable history of *every* change, and
- a **warehouse** — a current-state snapshot plus SCD2 history for point-in-time
  reconstruction (time travel),

with schema-drift safety, validation parity, and a dataset catalog.

The pipeline runs in two interchangeable modes that share the *same* lake + warehouse +
validation code:

- **Simulated, in-process** (default) — no external services; used by the test suite and
  CI gates.
- **Real PostgreSQL source via Docker** (optional) — insert/update/delete in a real
  Postgres and capture those changes (snapshot-diff CDC). See
  [TESTING_POSTGRES.md](./TESTING_POSTGRES.md).

The production analogue (Debezium / Postgres logical replication reading the WAL) is
documented at every layer; only the *capture* mechanism changes, never anything
downstream.

---

## 1. Architecture overview

```
            ┌──────────────┐   CDCRecord(seq)   ┌──────────────────────────────┐
 source ──▶ │ CDC capture  │ ─────────────────▶ │ lake  (append-only history)  │
 (Postgres  │  cdc.py /    │                    └───────────────┬──────────────┘
  or DuckDB │  bridge.py)  │                                    │ replay
  sim)      └──────┬───────┘                                    ▼
                   │ checkpoint (last seq)        ┌──────────────────────────────┐
                   └────────────────────────────▶ │ warehouse                    │
                                                  │  wh_*          (snapshot)    │
                                                  │  wh_*_history  (SCD2)        │
                                                  └───────────────┬──────────────┘
                                                                  ▼
                                            validation parity + catalog exposure
```

Every change becomes a `CDCRecord` carrying a monotonically increasing **`sequence`**
(the local analogue of a Kafka offset / Postgres LSN). `sequence` is the single ordering
and checkpoint primitive used by every layer.

---

## 2. Source schema design (`source/`)

| Table | Kind | PK | Foreign keys |
|---|---|---|---|
| `customers` | strong | `customer_id` | — |
| `products` | strong | `product_id` | — |
| `orders` | strong | `order_id` | `customer_id → customers` |
| `order_items` | weak | `order_item_id` | `order_id → orders`, `product_id → products` |
| `payments` | weak | `payment_id` | `order_id → orders` |

- **Strong vs weak:** strong entities have independent identity; `order_items` and
  `payments` have no meaning without their parent `order` (identity completed by the FK).
- **Types:** decimals for money (`price`, `total`, `amount`, `line_total`), timestamps,
  enums for `status`/`method`, and a nullable `settled_at`.
- **Enums:** customer/product/order/payment status domains + payment method.
- **Indexes:** FK columns and `updated_at` (the change-capture path).
- **Business invariants:** `order.total = Σ line_total`, `line_total = quantity *
  unit_price`, money ≥ 0, `price > 0`, `quantity > 0`, `settled_at ≥ created_at`, enum
  domains, and legal status transitions only.
- **Expected change patterns:** `customers`/`products` are mostly inserts with occasional
  status/price updates and rare deletes; `orders` are insert-then-update heavy (status
  walks `pending → paid → shipped → delivered`); `order_items` are insert-once and rarely
  change; `payments` are insert-then-update (`pending → settled`). The change-capture
  path (`updated_at` + diff) is indexed accordingly.

The contract lives once in `source/models.py` as `SCHEMA_CONTRACT` (a typed
`TableContract`/`ColumnSpec` structure). `source/postgres_ddl.sql` is the canonical
PostgreSQL DDL (ENUM types, PK/FK, CHECKs, indexes) that the DuckDB model mirrors, so the
same design holds whether running simulated or against real Postgres.

---

## 3. CDC capture (`ingestion/cdc.py`, `postgres/bridge.py`)

- A `CDCRecord` carries `operation` (insert/update/delete), `table`, `primary_key`,
  `before`/`after` row images, `captured_at`, and `sequence`.
- **Simulated:** `CDCCapture` is an in-process change log that assigns sequences and
  supports `records_since(offset)` for replay.
- **Real Postgres:** `bridge.sync()` reads the current state of every source table and
  diffs it against the last-seen snapshot stored in DuckDB (`_pg_snapshot`); the
  differences become insert/update/delete `CDCRecord`s. Sequencing continues
  monotonically across syncs.
- Both modes resume sequencing from the persisted checkpoint
  (`max(checkpoint, MAX(lake.sequence))`), so a crash between lake-write and checkpoint
  cannot reuse a sequence.

---

## 4. Checkpoint, replay & restart (`ingestion/checkpoint.py`)

- The last successfully applied `sequence` per consumer is persisted in
  `_ingest_checkpoint`.
- Each cycle processes only `records_since(checkpoint)` and advances the checkpoint
  **only after** a successful lake write + warehouse apply.
- Restart after a crash resumes from the last checkpoint; re-running with no new changes
  is a no-op.

---

## 5. Lake — full history (`lake/lake.py`)

- `lake_cdc_events` is **append-only**: every change is stored exactly once with its full
  row image as JSON.
- **Idempotent:** `sequence` is the primary key and appends use `ON CONFLICT DO NOTHING`,
  so duplicate/replayed events cannot corrupt history.
- It is the durable source of truth for replay and for rebuilding the warehouse.
- Production analogue: Parquet/Delta files on object storage, partitioned by table and
  capture date.

---

## 6. Warehouse — snapshot + time travel (`warehouse/warehouse.py`)

For every source table the warehouse keeps two coordinated models derived from the same
sequence-ordered stream (so they can never silently drift):

- **`wh_<table>` (current snapshot):** upsert by PK; deletes set `_deleted = true`
  (soft delete). `_cdc_seq` records the last applied sequence per row.
- **`wh_<table>_history` (SCD2):** `_valid_from_seq` / `_valid_to_seq` / `_is_current`
  versions, with `valid_from`/`valid_to` timestamps and the originating `_operation`.

Reconstruction:

- `reconstruct_as_of(table, seq)` — returns the rows valid at a given sequence
  (point-in-time / time travel) from SCD2 history.
- `restore_from_lake(target_seq)` — deterministically rebuilds the current snapshot by
  replaying the durable lake up to a sequence (operational rollback).

`apply_cdc_records` is **idempotent and out-of-order safe**: a record is skipped if the
row's current `_cdc_seq` is already ≥ its sequence.

---

## 7. Schema-change safety (`ingestion/schema_contract.py`)

Before every ingest cycle the live source schema is diffed against `SCHEMA_CONTRACT`:

- **Breaking → fail closed:** dropped/renamed column, type change, NOT-NULL tightening,
  or enum-domain shrink raises `SchemaIncompatibilityError` and **nothing is written**.
- **Additive → warn and continue:** brand-new columns or new enum values are reported but
  do not stop ingestion.

CI gate: `scripts/check_schema_contracts.py` (exits non-zero on a breaking change).

---

## 8. Validation parity (`validation/checks.py`)

The source's rules are re-asserted against the warehouse so downstream consumers are
protected even when the source is wrong:

- **System checks:** PK uniqueness, NOT-NULL, referential integrity (FKs), enum domains.
- **Business checks:** `order.total = Σ line_total`, `line_total = quantity *
  unit_price`, non-negative money, `price > 0`, `quantity > 0`, `settled_at ≥
  created_at`, and legal status transitions (validated from the ordered lake history).

CI gate: `scripts/run_data_quality_checks.py`.

---

## 9. Catalog exposure (`catalog/catalog.json`)

Every lake and warehouse dataset is registered with layer, description, owner,
consumers, update cadence, query path, and per-column schema.

CI gate: `scripts/validate_catalog.py`; a test also asserts the catalog stays in sync
with the live tables.

---

## 10. Reliability matrix

| Scenario | Handling |
|---|---|
| Duplicate / replayed event | Lake PK + `ON CONFLICT DO NOTHING`; warehouse skips `seq ≤ _cdc_seq` |
| Out-of-order event | Warehouse skips stale sequences; lake is keyed by sequence |
| Late-arriving update | Same `seq ≤ _cdc_seq` guard: a newer state already applied is never overwritten by an older one; the lake still records it |
| Retry after partial failure | Lake append + warehouse apply are idempotent; checkpoint advances only after both succeed, so a retry re-applies the same window safely |
| Delete | `delete` event → snapshot soft-delete + SCD2 version closed |
| Ingestion restart after checkpoint | Resume from persisted checkpoint; replay only `records_since(checkpoint)` |
| Incompatible schema change | Fail closed before any write |
| Recovery from historical data | `reconstruct_as_of` (SCD2) / `restore_from_lake` (replay from lake) |

---

## 11. Testing strategy (Red → Blue → Green)

`pytest -q` → **41 passing** tests across:

- `test_source_constraints.py` — PK/NOT-NULL/enum/FK/positive-amount constraints
- `test_cdc.py` — insert/update/delete capture, before/after images, replay
- `test_lake.py` — full retention, idempotent append, no duplication on re-ingest
- `test_warehouse.py` — latest snapshot, soft delete, SCD2 time travel, restore,
  out-of-order/duplicate safety
- `test_schema_safety.py` — breaking changes stop, additive changes warn
- `test_validation_parity.py` — system + business violations are caught
- `test_catalog.py` — catalog completeness and catalog ↔ live-table parity

---

## 12. How to run

Simulated (no external services):

```bash
cd submission/ecommerce-cdc
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

pytest -q                                   # full suite (41 tests)
python -m ingestion.pipeline                # end-to-end demo (ingest + time travel + restore)
python scripts/check_schema_contracts.py    # schema-safety gate
python scripts/run_data_quality_checks.py   # validation-parity gate
python scripts/validate_catalog.py          # catalog gate
```

Real PostgreSQL source (optional, Docker — full walkthrough in
[TESTING_POSTGRES.md](./TESTING_POSTGRES.md)):

```bash
docker-compose up -d                         # Postgres, schema auto-created
python scripts/pg_seed.py                    # insert realistic sample rows
python scripts/pg_sync.py                    # capture changes -> lake + warehouse + report
python scripts/pg_mutate.py                  # legal updates + a safe delete
python scripts/pg_sync.py                    # re-sync to see update/delete capture
```

---

## 13. Assumptions & trade-offs

- **DuckDB stands in for object storage + a cloud warehouse** to keep the solution
  runnable and dependency-free; the layering (lake vs warehouse) is what matters and maps
  cleanly to Parquet/Delta + BigQuery/Snowflake.
- **Snapshot-diff CDC** (in the Postgres bridge) is a polling approximation of log-based
  CDC; it is correct and idempotent, and the documented production path is Debezium / WAL
  with the LSN replacing `sequence`. Nothing downstream changes.
- **Row images are JSON** in the lake for schema flexibility; a columnar layout would be
  used at scale.
- **Checkpoint is per-consumer** to allow multiple independent downstream readers.
