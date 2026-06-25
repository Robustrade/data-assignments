# Pull Request — CDC Lakehouse Reliability (Wallet / Payments / Transfers)

## Summary
End-to-end CDC pipeline that mirrors a 6-table wallet/payments/transfers
source system into a **lake** (immutable change history) and a **warehouse**
(current-state + SCD2 history). It detects schema drift, stops the line on
incompatible changes, supports time-travel reconstruction and operational
restore, and enforces source-to-warehouse validation parity. Implemented in
Python + DuckDB and runnable in a single process.

Layered into six modules with narrow contracts:

```
source  →  cdc capture + orchestrator  →  lake  →  warehouse  →  quality  →  catalog
```

~1,400 LOC of pipeline + ~1,300 LOC of tests across **9 test modules**
covering modeling, CDC correctness, schema-change safety, warehouse
correctness, time travel, catalog metadata, data quality, and full end-to-end
scenarios.

See `ARCHITECTURE.md` for the layered design walk-through and
`ASSUMPTIONS.md` for tradeoffs and production analogues.

---

## Source Schema Design

- **Domain chosen:** wallet / payments / transfers
- **Strong entities:**
  - `customers` — registered platform users
  - `wallets` — balance accounts; one customer holds many (per-currency)
  - `transfers` — money movements between wallets; have business identity
- **Weak entities:**
  - `payment_methods` — saved card / bank / upi tied to a customer
  - `transfer_events` — state transitions for a transfer
  - `ledger_entries` — append-only credit/debit on a wallet
- **Keys, relationships, indexes:**
  - All primary keys are non-null `VARCHAR` natural identifiers
  - FKs: `payment_methods → customers`, `wallets → customers`,
    `transfers → wallets ×2 + payment_methods`,
    `transfer_events → transfers`, `ledger_entries → wallets + transfers`
  - 12 indexes covering CDC patterns (`updated_at`), referential lookups
    (`customer_id`, `transfer_id`), filtered scans (`status`), and
    audit-by-wallet (`(wallet_id, occurred_at)`)
- **Source validation rules** (enforced via `CHECK` and `NOT NULL`):
  - `wallet.balance >= 0`
  - `transfer.amount > 0`
  - `transfer.from_wallet_id != transfer.to_wallet_id`
  - `transfer.settled_at IS NULL OR settled_at >= created_at`
  - `ledger_entries.signed_amount` consistent with `direction` and `amount`
  - status enums: `customers.status`, `payment_methods.method_type` /
    `.status`, `wallets.currency` / `.status`, `transfers.status` /
    `.currency`, `transfer_events.to_status`, `ledger_entries.direction`

Schema is mirrored in versioned, frozen contracts at `source/contracts.py`,
which are the single source of truth for the schema detector and the
data-quality rules.

---

## CDC Strategy

- **How changes are captured:** `CDCCapture` wraps every `insert`,
  `update`, and `delete` call into a `CDCRecord` with operation, table,
  primary_key, before-image, after-image, monotonic `sequence`, and
  `captured_at`. The sequence is the LSN/Kafka-offset analogue and the
  primary correctness lever.
- **How inserts, updates, deletes are handled:**
  - **Insert** — adds a new current-state row with `_cdc_seq=record.sequence`
    and opens an SCD2 history row with `_is_current=true`,
    `_valid_from=captured_at`, `_op='insert'`.
  - **Update** — closes the open SCD2 row (`_valid_to=captured_at`,
    `_is_current=false`) and opens a new one with the post-image; updates
    the current-state row in place.
  - **Delete** — closes the open SCD2 row and appends a closed
    `_op='delete'` row (with the pre-image); flips current-state's
    `_deleted=true` (soft delete preserves analytics over deleted rows).
- **How replay/restart works:**
  - Per-stage checkpoint (`pipeline_checkpoints`): one row each for
    `lake` and `warehouse`. `run_tick` reads from
    `min(checkpoint_lake, checkpoint_warehouse)`, so a tick that wrote
    the lake but failed before the warehouse merge replays cleanly.
  - The lake's `sequence` PK silently drops duplicate appends.
  - The warehouse merge applies an event only when
    `record.sequence > existing._cdc_seq`, making it idempotent and
    out-of-order safe.
  - `set_checkpoint` refuses to move backwards — bugs surface loudly.
- **How duplicates are handled:** see above. Tested in
  `test_replay_does_not_double_apply`,
  `test_orchestrator_run_tick_is_idempotent`, and
  `test_duplicate_sequence_is_deduped`.

---

## Lake and Warehouse Modeling

- **How the lake captures every change:** single append-only table
  `lake_cdc_events(sequence PK, operation, table_name, primary_key,
  before_image JSON, after_image JSON, captured_at)`. One row per CDC
  event, no rewrite, no overwrite, no aggregation. `replay_records()`
  returns events in sequence order and is the warehouse's recovery feed.
- **How the warehouse maintains latest snapshot:** for each source
  table `T`:
  - `wh_T` — current-state with `_cdc_seq` (last applied sequence) and
    `_deleted` (soft delete). Same column shape as the source +
    `_cdc_seq` + `_deleted`.
  - `wh_T_history` — SCD2 with `(business_key, _valid_from)` PK and
    `_valid_to`, `_is_current`, `_op` columns. Every revision lands
    here; `_is_current=true` invariant: at most one row per PK.
- **How time travel / restore is supported:**
  - `state_at(conn, table, ts)` reconstructs `T` at any timestamp from
    `wh_T_history` using `_valid_from <= ts < _valid_to`. Excludes
    deleted rows automatically. Tested in `test_time_travel.py` against
    multiple inserted revisions.
  - `restore_current_state_to(conn, table, ts)` overwrites `wh_T` with
    the snapshot from `state_at(table, ts)` and **does not modify
    history** — restore is reversible, history is the system of record.
  - After restore, `_cdc_seq` resets to 0 so subsequent CDC events
    apply cleanly. Verified by `test_restored_state_accepts_new_cdc_records`.

---

## Schema Change Safety

- **How incompatible changes are detected:** `SchemaDetector` introspects
  the live source via `information_schema.columns` and
  `duckdb_constraints()` and compares against `source/contracts.py`.
  Eight rules:
  - **BREAKING:** `missing_table`, `column_dropped`, `type_changed`,
    `nullability_tightened`, `non_nullable_column_added`,
    `primary_key_changed`
  - **WARN:** `column_added` (additive), `enum_value_added` (drift
    surface, not necessarily breaking)
- **How ingestion is stopped:** at *two* levels:
  - **Orchestrator gate** — `run_tick(..., fail_on_drift=True)` runs
    `detector.check_all()` *before* reading any new events. On a
    breaking drift it returns `{aborted: true, reason:
    "schema_contract_violation", drift_summary: ...}` and writes
    nothing to the lake or warehouse. Tested in
    `test_run_tick_aborts_on_breaking_drift`.
  - **Capture gate** — `CDCCapture(detector, require_compatible_schema=True)`
    raises `SchemaContractViolation` on every write while a breaking
    drift is present. The capture log stays empty. Tested in
    `test_capture_with_strict_schema_refuses_writes_on_drop` and
    `test_capture_refuses_writes_when_strict`.
- **How warnings/failures are surfaced:**
  - `scripts/check_schema_contracts.py` is the CI entry; it exits
    non-zero with a printed drift summary on any breaking finding,
    suitable for the `schema-contract-check` GitHub status check.
  - `run_tick`'s aborted dict includes a `drift_summary` for log
    aggregation / Slack alerts.
  - WARN rules are reported via `report.warnings` and never abort —
    they are operational signals, not failures.

---

## Validation Parity

- **Which source system validations were mirrored downstream:**
  - **System rules** — auto-generated from `contracts.py`:
    - `sys.pk_unique.<table>` for every PK
    - `sys.not_null.<table>.<col>` for every NOT NULL column
    - `sys.enum.<table>.<col>` for every enum-domained column
    - `sys.fk.<table>.<col>` for every FK (ignoring `_deleted=true` rows)
  - **Business rules** — explicit:
    - `biz.wallet.balance_non_negative`
    - `biz.transfer.amount_positive`
    - `biz.transfer.no_self_transfer`
    - `biz.transfer.settled_at_after_created`
    - `biz.transfer.settled_implies_settled_at`
    - `biz.transfer.failed_implies_reason`
    - `biz.ledger.signed_amount_consistent`
    - `biz.ledger.balance_after_non_negative`
    - `biz.ledger.reconciles_to_wallet_balance` (monetary integrity:
      `SUM(signed_amount) per wallet == wh_wallets.balance`)
    - `biz.transfer_event.allowed_transition` (state machine validity)
- **How failures are checked and reported:**
  - `scripts/run_data_quality_checks.py:run_checks(conn)` returns
    `[(rule, count_of_failing_rows), ...]`. The CI script exits
    non-zero on any failure and prints the rule_id + offending count.
  - `tests/test_data_quality.py` proves both directions: the seeded
    warehouse passes all rules **and** every individual rule fires when
    its invariant is violated (tested for PK uniqueness, enum, FK,
    negative balance, settled_at backward, ledger consistency, ledger
    reconciliation, status transition).
  - A parity test (`test_every_contract_not_null_has_a_warehouse_rule`)
    blocks a contract change without a corresponding rule.

---

## Catalog Exposure

13 datasets in `catalog/catalog.json` covering every output the pipeline
produces:

- 1 lake dataset: `lake_cdc_events`
- 6 warehouse current-state: `wh_customers`, `wh_payment_methods`,
  `wh_wallets`, `wh_transfers`, `wh_transfer_events`, `wh_ledger_entries`
- 6 warehouse SCD2 history: each of the above + `_history`

Each dataset publishes:

- `name`, `layer` (`lake` / `warehouse`), `type`
  (`append-only-change-log` / `current-state` / `scd2-history`)
- `description`, `owner` (`data-platform`), `consumers` (per-dataset list:
  finance / treasury / fraud / audit / analytics / product / support)
- `update_cadence`, `sla_class` (money-critical datasets are `tier-1`)
- `tags` for governance (`monetary-integrity`, `pii`, `time-travel`, etc.)
- `primary_key` and `schema` / `schema_extension`

Discovery / access in this scope: consumers `cat catalog/catalog.json`
or `python -m scripts.validate_catalog` to verify it matches the
pipeline outputs. Production analogue is DataHub / Atlan / Unity
Catalog — the JSON shape is intentionally close to those tools'
dataset envelopes so the migration is a serializer swap.

`tests/test_catalog.py` enforces:
- catalog file exists and is valid JSON
- every expected dataset is declared (no missing, no extras)
- every dataset has the required descriptive fields
- controlled vocabularies for `layer`, `type`, `sla_class`
- consistency: lake → `append-only-change-log`, warehouse non-history
  → `current-state`, warehouse `_history` → `scd2-history`
- money-critical datasets are tier-1
- SCD2 history datasets are tagged for time-travel where they back
  balance lookups

---

## Validation

Local validation commands run in order:

```bash
cd submission/wallet-payment-transfer
pip install -r requirements.txt

# 1. Schema contract gate (BREAKING drifts → exit 1)
python -m scripts.check_schema_contracts

# 2. Data quality parity gate
python -m scripts.run_data_quality_checks

# 3. Catalog metadata gate
python -m scripts.validate_catalog

# 4. Full test suite (~80 tests, < 10s)
pytest -q

# 5. End-to-end pipeline demo
python -m scripts.run_pipeline

# 6. Time travel demo (state_at at multiple timestamps)
python -m scripts.demo_time_travel

# 7. Schema-change / stop-the-line demo
python -m scripts.demo_schema_change
```

All five CI gates exit non-zero on failure and are mapped 1-1 to the
checks listed in `github_pr_checks_data.md`.

---

## Known Limitations / Next Steps

- **Replace `CDCCapture` with a Debezium stub** — the current shape is
  Debezium-faithful (envelope columns, before/after, sequence == LSN)
  but does not exercise transactional grouping or snapshot-vs-streaming
  modes. With a stub we could test "snapshot then switch to streaming"
  explicitly.
- **Column-level lineage in the catalog** — datasets know their PKs but
  not which warehouse columns derive from which source columns. An
  OpenLineage emitter is the natural addition.
- **Freshness rules** — current data-quality covers correctness, not
  staleness. A `dq.freshness.<table>` rule comparing `MAX(_cdc_seq)`
  against expected cadence is a one-day add.
- **Containerization** — a `Dockerfile` + `docker-compose.yml` running
  Postgres + Kafka + this orchestrator would make the production
  analogue concrete. The contracts in `pipeline/cdc.py` and
  `pipeline/schema_detector.py` are designed to swap targets.
- **Automatic non-breaking migration** — for `column_added` (additive)
  drift we currently emit a WARN; we could instead auto-extend the
  warehouse table inside the same tick. Out of scope by assignment.

None of these change the architecture; they are additive.

---

## Responsible AI Usage

- **Did you use AI tools?** Yes — Claude Code as a pair-programming
  agent.
- **Where did they help?**
  - Drafting boilerplate (DDL strings, frozen-dataclass contracts, JSON
    catalog entries) and the per-rule data-quality SQL.
  - Surfacing cross-cutting concerns (the `_cdc_seq` guard handles both
    duplicate replay AND out-of-order arrival; `restore_current_state_to`
    needs to reset `_cdc_seq=0` so subsequent CDC applies cleanly).
  - Generating test scaffolds and parametrizations.
- **What did I personally verify or correct?**
  - The strong/weak entity classification — I made the calls based on
    lifecycle ownership, not the LLM's first guess.
  - The seed reconciliation — initial balances did not match
    `SUM(signed_amount)`; I worked out the funding entries explicitly so
    the monetary-integrity rule passes.
  - The `_cdc_seq` guard inequality — I changed it to **strictly greater**
    so the same-sequence replay is correctly a no-op (otherwise replay
    would re-open SCD2 rows).
  - The orchestrator's drift gate placement — checked **before** reading
    any events and **before** advancing checkpoints, so partial state is
    impossible on a breaking drift.
  - Refactored `CDCCapture` to expose `captured_at` as a public
    optional parameter rather than letting the time-travel demo poke
    `_record(...)` directly.
  - Authored every test by walking through the failure mode I wanted
    to prove (Red), confirming the test failed against an unfixed
    implementation, then verifying it passed (Blue → Green).
  - Structured the catalog tests around controlled vocabularies and
    money-critical SLA rules, not just "field exists" assertions.

The architecture decisions, contract shape, severity classifications,
and the layered file structure are all my own. The code is reviewed
end-to-end and every test asserts a behavior I considered worth
proving.
