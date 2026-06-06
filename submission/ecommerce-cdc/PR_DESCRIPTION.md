# CDC Lakehouse Reliability — E-commerce

A small but deeply correct CDC pipeline keeping a lake (full history) and a warehouse
(latest snapshot + time travel) in sync with an e-commerce source.
Stack: PostgreSQL (source design) · Python (ingestion) · DuckDB (lake + warehouse) ·
pytest. CDC is simulated in-process (dependency-free); the production analogue is
documented throughout. Full design: `submission/ecommerce-cdc/SOLUTION_APPROACH.md`.

## 1. Source schema design
- **Tables:** `customers`, `products`, `orders` (strong); `order_items`, `payments` (weak).
- **Strong vs weak:** strong entities have independent identity; `order_items` and
  `payments` have no meaning without their parent `order` (identity completed by the FK).
- **Keys/relationships:** PKs on every table; FKs `orders→customers`,
  `order_items→orders/products`, `payments→orders` (1—N throughout).
- **Indexes:** FK columns + `updated_at` (the change-capture path).
- **Types:** decimals (`price/total/amount/line_total`), timestamps, enums
  (`status/method`), nullable (`settled_at`).
- **Validation rules:** `total = Σ line_total`, `line_total = qty*unit_price`,
  non-negative money, `price>0`, `quantity>0`, `settled_at ≥ created_at`, enum domains,
  legal status transitions.

## 2. CDC strategy
- Every mutation → a `CDCRecord(op, table, pk, before, after, captured_at, sequence)`.
- **`sequence`** is a monotonic offset (Kafka offset / Postgres LSN analogue) — the sole
  ordering + checkpoint primitive.
- **Replay/restart:** checkpoint stores the last applied sequence; restart replays only
  `records_since(checkpoint)`.
- **Duplicates:** lake PK + `ON CONFLICT DO NOTHING`; warehouse skips events whose
  sequence ≤ the row's current `_cdc_seq` (also handles out-of-order).
- **Deletes:** captured as `delete` events; snapshot soft-deletes, SCD2 history closes
  the version.

## 3. Lake & warehouse modeling
- **Lake (`lake_cdc_events`):** append-only, idempotent, stores full row image as JSON —
  every change is recoverable.
- **Warehouse current (`wh_*`):** upsert by PK + `_deleted` soft-delete = latest snapshot.
- **Warehouse history (`wh_*_history`):** SCD2 (`_valid_from_seq`/`_valid_to_seq`/
  `_is_current`).
- **Time travel:** `reconstruct_as_of(table, seq)` reads the version valid at `seq`.
- **Restore:** `restore_from_lake(target_seq)` deterministically rebuilds the snapshot by
  replaying the lake.

## 4. Schema change safety
- Declared `SCHEMA_CONTRACT` (columns, types, nullability, enum domains, keys).
- Each cycle diffs live source schema vs contract. **Breaking** (dropped/renamed column,
  type change, nullable tightening, enum shrink) → raise `SchemaIncompatibilityError`,
  **stop ingestion, write nothing**, exit non-zero. **Additive** changes warn and continue.
- CI gate: `scripts/check_schema_contracts.py`.

## 5. Validation parity
- **System:** PK uniqueness, not-null, referential integrity, enum domains.
- **Business:** total = Σ line_total, line_total = qty*unit_price, non-negative money,
  `price>0`, `quantity>0`, `settled_at ≥ created_at`, legal status transitions (validated
  from ordered lake history).
- Surfaced via `validation/checks.py`, `scripts/run_data_quality_checks.py`, and tests.

## 6. Catalog exposure
- `catalog/catalog.json` registers every lake + warehouse dataset (layer, owner,
  consumers, cadence, query path, per-column schema). Gate: `scripts/validate_catalog.py`;
  a test also asserts catalog ↔ live tables stay in sync.

## 7. Responsible AI usage
AI assistance was used to scaffold boilerplate and draft documentation. All schema
design, CDC/SCD2 semantics, schema-safety rules, and validation logic were reviewed and
verified by running the test suite (`41 passed`) and the CI gate scripts locally.

## Testing
`pytest -q` → **41 passed**, covering modeling/constraints, CDC correctness
(insert/update/delete/duplicate/replay/restart), schema-change safety, warehouse
snapshot + time travel + restore, and catalog. Red→Blue→Green discipline followed.
