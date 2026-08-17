# Design Document — CDC Lakehouse Reliability Assignment

Domain: **Wallet / Payments / Transfers**

This document is written before implementation, per the assignment's
documentation-first requirement.

## 1. Source Schema

5 tables. See `src/source_db.py::SCHEMA_SQL` for full DDL (SQLite dialect;
`01_source_schema.sql` in the repo root has the equivalent PostgreSQL DDL
with native enums and `COMMENT ON` extended properties, representing the
intended production schema).

| Table | Strong/Weak | Why |
|---|---|---|
| `customers` | Strong | Independent identity (email), independent lifecycle. |
| `wallets` | Strong | Owns its own balance/status lifecycle; FK to customer but has independent business meaning, not just an attribute. |
| `transactions` | Strong | Independently meaningful record with its own lifecycle (PENDING → COMPLETED/FAILED → REVERSED). |
| `transaction_line_items` | Weak | Composite PK `(transaction_id, line_number)`; cannot exist without parent transaction; cascades on delete. |
| `balance_history` | Weak | Composite PK `(wallet_id, history_id)`; append-only ledger, dependent on wallet. |

Invariants: `wallets.balance >= 0`; `transactions.amount > 0`;
`settled_at >= initiated_at`; `SUM(line_items.amount) == transaction.amount`;
`wallets.balance == latest balance_history.balance_after`.

## 2. CDC Contract (Simulated, documented)

**What's simulated:** Production would use PostgreSQL logical replication
(WAL) consumed by a Debezium connector into Kafka. Locally, we simulate the
same *contract* — an append-only, ordered, durable change log — via a
`cdc_log` table that every DML call writes to in the same transaction as the
business row (`src/source_db.py::SourceDB`). This preserves the properties
that matter for grading: ordering (monotonic `log_id`), durability (same
transaction), and completeness (no DML path bypasses it).

**What would change in production:** the extractor would consume a Kafka
topic instead of polling a SQLite table; offsets would be Kafka consumer
group offsets instead of a JSON checkpoint file; multiple consumers could
run in parallel instead of a single-threaded poll.

**Capture:** insert/update/delete on any tracked table → one `cdc_log` row
(table, op, pk, full row snapshot, event_ts).

**Extraction:** `cdc_extractor.py` reads `cdc_log` where `log_id > checkpoint`,
writes each event as one JSON line to `lake/<table>/dt=<date>/changes.jsonl`,
advances the checkpoint only after a successful flush.

**Replay / restart:** checkpoint is file-based (`checkpoints/cdc_checkpoint.json`).
Restarting after a crash resumes from the last saved `log_id`. Re-running
against a rolled-back checkpoint safely re-emits already-seen events into the
lake (harmless — lake is append-only); the warehouse loader deduplicates by
`log_id` via an `_applied_log_ids` table, so double-delivery to the lake never
double-applies to the warehouse.

**Deletes:** captured as `op = 'DELETE'` events carrying just the PK; the
warehouse loader closes the row's SCD2 history version with `is_deleted = TRUE`
and removes it from the `_current` table.

## 3. Lake and Warehouse Modeling

- **Lake** (`lake/<table>/dt=YYYY-MM-DD/changes.jsonl`): append-only JSONL,
  one line per change event, never rewritten. This is the full, replayable
  history of everything that ever happened in the source.
- **Warehouse** (`data/warehouse.duckdb`): two structures per source table:
  - `<table>_current` — latest snapshot, one row per business key, used by
    downstream analytics.
  - `<table>_history` — SCD2 versions (`valid_from`, `valid_to`, `is_current`,
    `is_deleted`), one row per version of the entity.

**Time travel:** `reconstruct_at(table, pk, as_of_ts)` queries `_history`
for the version whose `[valid_from, valid_to)` window contains `as_of_ts`.
This is how a prior point in time is restored, and it's the mechanism a
rollback would use operationally (replay lake events up to a target
timestamp into a fresh warehouse, or simply query `_history` directly for
read-only point-in-time views).

## 4. Schema Change Safety

`schema_guard.py` fingerprints the source schema (per-table column name,
type, nullability, PK role) via `PRAGMA table_info` and persists the
last-known-good fingerprint. Every pipeline run compares current vs. known-good
**before** any extraction happens.

**Incompatible (halts ingestion):** dropped/renamed column, type change,
nullability change, PK-role change, table removal.
**Compatible (does not halt):** new tables, new columns — treated as
additive and non-breaking for this scope (documented simplification).

On incompatible drift: `SchemaDriftError` is raised, a structured line is
appended to `checkpoints/schema_alerts.log`, and — critically — the
extractor's schema check runs *before* any lake write, so nothing is written
under broken assumptions. The pipeline halts, not silently degrades.

## 5. Validation Parity

Source enforces PK/FK/CHECK constraints natively (SQLite `PRIMARY KEY`,
`REFERENCES`, `CHECK`). `validations.py` re-asserts the same rules against
the warehouse's `_current` tables: PK uniqueness, referential integrity,
non-negative balances, positive transaction amounts, line-items-sum-matches-
transaction-amount, settled-after-initiated. Failures are returned as a
structured report (`{check_name: [violation, ...]}`) and `validations.py`
run standalone exits non-zero on any failure, suitable for CI gating.

## 6. Catalog Exposure

`catalog.py` generates `catalog/catalog.yml` listing every lake and
warehouse dataset with: layer, path, format, schema reference, owner,
intended consumers, update cadence, primary key. Regenerated idempotently
each run. Production analogue: this would be auto-registered entries in a
Glue/Unity Catalog/DataHub instance, updated by the pipeline on every
successful run instead of by manual invocation.

## 7. Reliability Notes

- **Duplicates:** deduped at warehouse-load time via `log_id` tracked in
  `_applied_log_ids`.
- **Retries after partial failure:** checkpoint only advances after a
  successful write; a crash mid-batch simply re-processes from the last
  saved offset on the next run.
- **Out-of-order arrival:** `log_id` is monotonic and assigned at the source
  transaction boundary, so ordering is guaranteed by construction within this
  design; true out-of-order arrival (e.g. multi-partition Kafka) is called
  out as a known gap addressed by out-of-order buffering per key in production.
- **Restart after checkpoint:** demonstrated in `tests/test_cdc_correctness.py::test_restart_from_checkpoint_after_partial_failure`.
- **Deletes:** explicitly modeled end-to-end (source → lake → warehouse `is_deleted`).
