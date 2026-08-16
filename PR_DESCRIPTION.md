# CDC Lakehouse Reliability Assignment — Submission

Domain: **Wallet / Payments / Transfers**. Full design writeup in `DESIGN.md`
(written before implementation). Setup/run instructions in `README.md`.

## 1. Source Schema Design

5 tables, mixing strong and weak entities (`01_source_schema.sql` has the
production-target PostgreSQL DDL with native enums and `COMMENT ON` extended
properties; `src/source_db.py` has the working SQLite equivalent used for
local execution):

- **Strong:** `customers`, `wallets`, `transactions` — each has independent
  identity and lifecycle.
- **Weak:** `transaction_line_items` (composite PK `transaction_id,
  line_number`, `ON DELETE CASCADE`), `balance_history` (composite PK
  `wallet_id, history_id`, append-only ledger) — neither can exist without
  its parent.

Keys/relationships: FKs from `wallets→customers`, `transactions→wallets`
(nullable, for one-sided external flows), `line_items→transactions`,
`balance_history→wallets`/`transactions`. Indexes on all FK columns plus a
unique `(customer_id, currency_code)` index on wallets. Validation rules:
`balance >= 0`, `amount > 0`, `settled_at >= initiated_at`, enum-constrained
status/type fields, plus cross-row invariants (line items sum = transaction
amount) enforced downstream.

## 2. CDC Strategy

Changes are captured via an application-level append-only `cdc_log` table
written in the same transaction as every business DML (insert/update/delete
all go through a `SourceDB` wrapper — no path bypasses logging). This is a
**documented simulation** of a WAL/Debezium-style log; production would swap
this for PostgreSQL logical replication into Kafka (see `DESIGN.md` §2 for
the full production-vs-local mapping).

- **Replay/restart:** file-based checkpoint (`checkpoints/cdc_checkpoint.json`)
  storing the last-processed `log_id`; restart resumes from there, not from
  scratch. Demonstrated in `tests/test_cdc_correctness.py::test_restart_from_checkpoint_after_partial_failure`.
- **Duplicates:** deduplicated at warehouse-load time via a `_applied_log_ids`
  table keyed on `log_id` — re-delivering the same lake events is a no-op.
  Demonstrated in `test_replay_does_not_duplicate_in_warehouse`.
- **Deletes:** captured as `DELETE` events, propagated through the lake, and
  applied in the warehouse by closing the SCD2 history version
  (`is_deleted=TRUE`) and removing the row from the `_current` snapshot.

## 3. Lake and Warehouse Modeling

- **Lake:** append-only, date-partitioned JSONL (`lake/<table>/dt=.../changes.jsonl`),
  one line per change event — full, replayable history, never rewritten.
- **Warehouse (DuckDB):** two tables per source entity — `<table>_current`
  (latest snapshot, one row per business key) and `<table>_history` (SCD2:
  `valid_from`/`valid_to`/`is_current`/`is_deleted`).
- **Time travel:** `warehouse_loader.reconstruct_at(table, pk, as_of_ts)`
  queries `_history` for the version whose validity window contains
  `as_of_ts`. Demonstrated in `test_historical_reconstruction_time_travel`,
  which updates a wallet balance and proves the pre-update value is still
  reconstructable while `_current` reflects the new value.

## 4. Schema Change Safety

`schema_guard.py` fingerprints the source schema (per-column name, type,
nullability, PK role) and compares against a persisted last-known-good
baseline **before any extraction runs**. Dropped/renamed columns, type
changes, nullability changes, and PK-role changes are classified
incompatible → `SchemaDriftError` is raised, a structured alert is appended
to `checkpoints/schema_alerts.log`, and — because the check runs before any
lake write — nothing is written under broken assumptions. New tables/columns
are treated as additive/non-breaking (documented simplification). Covered by
`tests/test_schema_safety.py` (5 tests: baseline establishment, compatible
pass-through, dropped-column halt, alert content, type-change detection).

## 5. Validation Parity

`validations.py` re-asserts the source's system rules (PK uniqueness, FK
integrity, non-null) and business rules (non-negative balance, positive
amount, line-items-sum = transaction amount, settled≥initiated) against the
warehouse `_current` tables. Returns a structured `{check: [violations]}`
report; run standalone it exits non-zero on any failure, so it can gate CI.
Covered in `tests/test_warehouse_correctness.py`, including a test that
deliberately creates a line-item mismatch and asserts it's caught.

## 6. Catalog Exposure

`catalog.py` generates `catalog/catalog.yml`, registering every lake and
warehouse dataset with layer, path/format, schema reference, owner, intended
consumers, update cadence, and primary key. Regenerated idempotently on each
pipeline run. Production analogue: auto-registration into a Glue/Unity
Catalog/DataHub instance.

## 7. Responsible AI Usage

I used Claude (Anthropic) to help design and implement this pipeline against
the assignment spec — it drafted the schema, CDC/warehouse/validation code,
tests, and documentation from requirements I gave it.

What I reviewed and validated myself:
- Ran the full pipeline end-to-end locally (seed → extract → load → catalog
  → validate) and confirmed it completes without error.
- Ran the full test suite (17 tests, all passing) and read through each test
  to confirm it actually exercises the behavior it claims to (e.g., the
  schema-drift tests genuinely mutate the SQLite schema and assert the halt,
  rather than mocking the check).
- Reviewed the schema-drift detection logic specifically, since it's the
  most safety-critical piece — confirmed it runs before any lake write and
  that the incompatible-change classification list matches what the
  assignment calls out (dropped/renamed column, type change, enum/nullability
  change).
- Manually triggered a schema break (`ALTER TABLE ... DROP COLUMN`) outside
  the test suite and confirmed the CLI halts with a clear message and the
  alert log is written (steps documented in `README.md`).
- Decided on the domain (wallet/payments), the simulated-CDC approach (vs.
  attempting real Postgres WAL locally), and the SCD2-based time-travel
  design as tradeoffs appropriate for the assignment's time-box; these are
  documented explicitly in `DESIGN.md` rather than left implicit.

## Assumptions, Tradeoffs, Limitations

See `README.md` → "Assumptions & Limitations" and `DESIGN.md` §2/§7 for the
full list (simulated CDC vs. real WAL, backward-compatible schema changes
not halting, out-of-order arrival across independent producers not fully
solved, no production orchestrator).
