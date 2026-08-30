# CDC Lakehouse — order-to-cash (Mayur Bhavsar)

## Where to find each requirement

| Requirement | Where |
|---|---|
| Source schema design | `sql/001_source_schema.sql`, `docs/01-source-model.md` |
| CDC implementation | `sql/002_cdc_triggers.sql`, `cdclake/ingest.py`, `docs/02-cdc-contract.md` |
| Lake output | `cdclake/lake.py`, `lake/cdc_events/…` |
| Warehouse models | `models/`, `macros/warehouse.sql` |
| Schema change safety | `contracts/`, `cdclake/gate.py`, `docs/03-schema-evolution.md` |
| Time travel / restore | `cdclake/warehouse.py`, `docs/04-time-travel.md` |
| Validation parity | `models/schema.yml`, `dbt_tests/`, `cdclake/quality.py`, `docs/05-validation-parity.md` |
| Catalog | `catalog/datasets.yml`, `docs/06-catalog.md` |
| Setup + how to run | `README.md`, `Makefile` |
| Architecture and trade-offs | `docs/00-architecture.md`, `docs/adr/` |
| Operations | `docs/runbook.md` |

---

## Summary

A CDC pipeline over an order-to-cash source. Database triggers capture every insert, update and delete; an ingestion runner gates them against a published schema contract and lands them in an append-only lake; dbt derives SCD2 history, current-state views and an analytical mart in DuckDB. The pipeline halts and stays halted when the source schema changes incompatibly, and any prior point in time can be reconstructed and written back to the source.

Runs with no services: `pip install -r requirements.txt && make demo`.

Four properties everything else follows from, each asserted by tests:

1. **The warehouse is a pure function of the lake.** No model reads the source; ordering is by `event_id`, never arrival or wall clock. Replay is idempotent and out-of-order arrival cannot change the result.
2. **The checkpoint is derived from the lake manifest, not stored beside it.** No second source of truth to drift.
3. **The lake schema does not change when the source schema changes.** `pk`, `before` and `after` are JSON text, so drift cannot corrupt the change log.
4. **Incompatible drift stops the line and stays stopped** across restarts, until a human acknowledges it.

---

## Source Schema Design

**Domain:** order-to-cash for a small e-commerce business — chosen because its invariants are real (line-item totals, legal status transitions, settlement ordering) rather than decorative.

```
customer ──1:N──▶ customer_order ──1:N──▶ order_item ◀──N:1── product
                        │
                        └──1:N──▶ payment ──1:N──▶ payment_attempt
```

**Strong entities:** `customer`, `product`, `customer_order`, and `payment`.

**Weak entities:** `order_item` — PK `(order_id, line_no)` — and `payment_attempt` — PK `(payment_id, attempt_no)`. Both fail the two tests for strength together: identity depends on the parent (the child key is meaningless alone) and existence depends on it (`ON DELETE CASCADE`).

`payment` is deliberately **not** weak. `payment_id` is unique on its own, and it does not cascade — deleting an order should never silently destroy financial records, so the foreign key forces an erasure to walk the graph explicitly. That asymmetry is exercised by the delete workload and asserted in `test_cascaded_child_deletes_are_captured`.

**Keys and relationships:** 6 tables, 5 foreign keys, 2 composite primary keys, 2 unique constraints (`customer.email`, `product.sku`).

**Indexes** — each for a stated pattern, and pinned by a test: `customer_order (customer_id, placed_at)` for the commonest lookup; `customer_order (updated_at)` for the watermark fallback path; `order_item (product_id)`, `payment (order_id)`, `payment (status, created_at)`, `payment_attempt (result, attempted_at)`, `cdc_events (table_name, event_id)` for the consumer's forward scan.

**Types:** `DECIMAL(12,2)` money, ISO-8601 UTC timestamps, `CHECK ... IN (...)` enum domains, nullable optional attributes each carrying a conditional rule.

**Validation rules:** 5 enum domains, 5 monetary non-negativity checks, an `extended_amount = quantity * unit_price` check, conditional-nullable rules on `cancelled_at`, `settled_at` and `failure_code`, plus a `CHECK` on `cdc_events` that enforces the change log's own shape. Two rules — order totals matching line items, and legal status transitions — cannot be expressed as source constraints at all, which is precisely why the warehouse re-asserts them.

Full detail: `docs/01-source-model.md`.

---

## CDC Strategy

**How changes are captured.** 18 `AFTER INSERT / UPDATE / DELETE` triggers write a row into `cdc_events` inside the source transaction: `event_id` (autoincrement, the replay offset), `table_name`, `op`, canonical `pk` JSON, full before and after images, `committed_at`. Being database-level, capture cannot be bypassed by an application that forgets to emit an event, and it sees deletes — which a timestamp watermark never can.

It is behind a `CdcSource` protocol. A WAL/Debezium backend implements the same two methods and nothing downstream changes; ADR-0001 covers what that migration involves, including the one assumption that must not survive it (see below).

**Delivery semantics, in one sentence:** transport is at-least-once; the lake write is idempotent under the `(table_name, event_id)` merge key; the checkpoint advances only after the data files are durable — so a crash produces a duplicate, never a loss, and the duplicate is absorbed downstream. Net effect: effectively-exactly-once.

**How replay and restart work.** The checkpoint is not stored; it is `max(checkpoint_after)` over the lake manifest, so it cannot disagree with the data. A restarted process resumes exactly where the last durable batch ended. A crashed batch is retried with the same batch id, so it overwrites its own partial files rather than accumulating orphans. Full replay from offset zero rebuilds an identical warehouse.

**How duplicates are handled.** `stg_cdc_events` deduplicates on `(table_name, event_id)`. `test_duplicate_delivery_is_absorbed_by_the_warehouse` writes every record a second time and proves the warehouse cannot tell.

**How ordering is handled.** By `event_id`, never arrival time. `test_out_of_order_arrival_produces_the_same_warehouse` lands the same records in reverse order in a fresh workspace and asserts identical current state.

**How deletes are handled.** As tombstones. The `op = 'D'` record stays in the lake forever; the SCD2 model closes the interval and emits a version with `is_deleted = true` carrying the last known values via `coalesce(after, before)` — without that, the moment before the delete would be unrecoverable. Cascaded child deletes are captured too, which required `PRAGMA recursive_triggers`; without it SQLite fires no trigger for a cascade and the children would vanish silently.

**One thing worth flagging.** `event_id` is a safe offset *here* because SQLite serialises writers, so ids are assigned in commit order. On Postgres a sequence value is allocated at statement time but visible at commit, so a long transaction can commit id 100 after a short one commits 101 — and a consumer scanning `id > 101` would skip it forever. Migrating the engine without moving the offset to the WAL LSN would introduce silent data loss. This is written up in `docs/02-cdc-contract.md`.

---

## Lake and Warehouse Modeling

**How the lake captures every change.** One immutable NDJSON file per `(batch, table)`, partitioned `table_name=… / ingest_date=…`, written to a temporary name and atomically renamed, followed by an fsynced append to `lake/_manifest/batches.ndjson`. Readers resolve files **through the manifest**, so a partial write from a crash is invisible rather than half-visible. Nothing in `cdclake/lake.py` can modify or delete a committed record.

`pk`, `before` and `after` are stored as JSON **text**, not nested structs. This is deliberate: the physical lake record has the same eleven columns forever, so a source schema change produces different JSON rather than a different file layout, and drift can never corrupt the change log or the ability to replay history recorded under an older contract.

**How the warehouse maintains the latest snapshot.**

```
stg_cdc_events            typed, deduplicated on (table_name, event_id)
  └── dim_*_history       SCD2: valid_from / valid_to / is_current / is_deleted
        └── *_current     view: where is_current and not is_deleted
              └── mart_order_financials
```

Current-state models are **views** over history, so the two cannot disagree. History models are tables, because the window functions are the expensive part. 14 models in total.

**How time travel and restore are supported.** Two primitives:

```python
warehouse.as_of(entity, "2026-08-29T18:29:00.000Z")   # by time
warehouse.as_of_version(entity, 44)                    # by change version
```

Both resolve by ranking `_version` descending rather than testing a `valid_from <= t < valid_to` range, which would silently skip the zero-length interval produced when two changes to the same row share a commit timestamp.

There are two primitives because a timestamp genuinely cannot separate two changes committed in the same millisecond — `as_of` gives a defined answer there (the later version, deterministically) but only the version can land *between* them. Same distinction Delta Lake draws between `TIMESTAMP AS OF` and `VERSION AS OF`; ADR-0003 records how a flaky test surfaced it.

A restore is not a report: `test_restore_output_can_be_written_back_to_the_source` reads a deleted order out of history, writes it back into a source that still enforces every constraint, and then asserts the restore itself flows back through CDC into the warehouse.

---

## Schema Change Safety

**How incompatible changes are detected.** Before every batch — before a single row is read — the live schema is introspected and diffed against `contracts/<table>.json` using an explicit policy matrix, never ad hoc judgement.

| COMPATIBLE (warn, continue) | BREAKING (halt) |
|---|---|
| Add a column | Drop a column |
| Widen decimal precision, same scale | Rename a column |
| Add an enum value | Any other type change |
| Remove an index | Either direction of nullability change |
| Add a table | Remove an enum value |
| | Change a primary key, foreign key or unique constraint |
| | Drop a contracted table |

Two rules that are usually got wrong, and are argued in `docs/03-schema-evolution.md`:

- **Both nullability directions are breaking.** `nullable → NOT NULL` breaks writers and backfill; `NOT NULL → nullable` breaks every downstream not-null assertion, and fails at build time rather than ingest time, which is a worse place to find out.
- **A rename is indistinguishable from a drop plus an add.** Without a mapping hint there is no evidence `full_name` *became* `customer_name`. The gate detects the shape, says in the finding that it cannot tell the difference, and halts for a human decision.

**How ingestion is stopped.** Not an exception — a persisted state machine. `RUNNING → HALTED(reason, checkpoint, diff, detected_at)` is written to `_state/pipeline_state.json`, and `guard_not_halted` runs first in every subsequent batch, so **a scheduled retry five minutes later is refused**. Getting out requires `republish-contracts` (deliberate, separate — accepting a breaking change is a decision about the meaning of the data) plus `acknowledge --operator --note`, which records who cleared it and why.

**How warnings and failures are surfaced.** An `ERROR` record on halt and a `WARN` record on compatible drift, appended to `_state/alerts.ndjson` — the webhook payload in production, a file here so that tests can assert a warning was actually *surfaced* rather than assume a log line was written. The CLI exits non-zero.

**Proof that the gate fires.** Eight schema mutations ship in `cdclake/source.py` and are applied to throwaway databases: five must be classified BREAKING, three must be COMPATIBLE *and still produce a warning*. Checked by `scripts/check_schema_contracts.py`, by 21 parametrised pytest cases that also assert the checkpoint did not move and no partial files were written, and demonstrated end to end by `make stop-the-line`.

---

## Validation Parity

**Which validations were mirrored.** All of them, and the pairing is mechanical rather than asserted in prose. `cdclake/quality.py:check_validation_parity` reads `contracts/*.json` and `models/schema.yml` and derives what must exist:

- every non-nullable column ⇒ a `not_null` assertion on `<entity>_current`;
- every enum domain ⇒ an `accepted_values` assertion whose value set is **exactly equal** to the contract's, not merely present;
- every single-column unique constraint and primary key ⇒ `unique`;
- every single-column foreign key ⇒ `relationships` pointing at `ref('<parent>_current')` on the right field.

That is 65 parity checks. If someone adds a `CHECK ... IN (...)` to the source next quarter and forgets `schema.yml`, CI goes red.

Business rules the source **cannot** enforce are re-asserted as singular dbt tests: order total versus summed line items, legal status transitions, `settled_at >= created_at`, conditional-nullable rules, non-negative money. Status-transition validation is only expressible because the warehouse keeps history — a snapshot-only warehouse can check a state but never the path taken to it.

Full traceability matrix: `docs/05-validation-parity.md`.

**How failures are checked and reported.** 127 in-warehouse assertions fail `dbt build` with the model, the test and the failing rows. 87 platform checks (reconciliation, lake completeness, manifest integrity, checkpoint chaining, freshness against a stated 60-second SLO, source integrity, and the 65 parity checks) print per-check results and fail `make quality`. Nothing here is a warning that scrolls past; every one fails a build.

**And the checker is tested for its ability to fail.** `tests/test_validation_parity.py` mutates a copy of `schema.yml` four ways — a removed `not_null`, a narrowed enum domain, a removed `relationships`, a removed `unique` — and asserts each is caught. A parity check that only ever passes proves nothing.

---

## Catalog Exposure

`catalog/datasets.yml` publishes 15 datasets (1 lake, 14 warehouse). Every entry is required to carry owner (resolved against an `owners:` block with contact and on-call channel), description, intended consumers, update cadence, storage, location, access snippet, PII flag and lineage. Entity schemas use `columns_from_contract`, so the catalog cannot describe a different schema from the one the contract publishes.

`lake.cdc_events` additionally carries `not_intended_for` — "analytics; it is a change log, not a snapshot, and querying it for current state will double-count updated rows" — because that is the misuse that actually happens. Ownership splits along the same line: the platform team owns the change log and history models (correctness surfaces), analytics owns current-state views and marts (product surfaces).

PII is flagged per dataset, including on the lake, with a note that before/after images carry customer email, name and phone so erasure obligations reach the lake and not only the warehouse — the consequence of "keep every change" that is most often missed.

`scripts/validate_catalog.py` checks the publication against reality: required metadata present; catalogued lake schema equal to the physical record; catalog and dbt models agreeing **in both directions** (the reverse check is the one teams skip — it stops a model shipping undocumented); and every catalogued column set equal to the live relation, read from `information_schema` after a real build. Both failure directions are themselves tested.

Production analogue — Unity Catalog / Glue / DataHub — in `docs/06-catalog.md`.

---

## Validation

Everything below was run from a clean state on Python 3.11, dbt-core 1.12.3, dbt-duckdb 1.11.0, DuckDB 1.5.5.

| Command | Result |
|---|---|
| `make test` | **89 passed** in ~45s |
| `make quality` | **87/87** platform checks; `dbt build` **PASS** (14 models, 127 assertions) |
| `make contracts` | **PASS** — contracts match the DDL, generated spec current, all 8 mutations classified correctly |
| `make catalog` | **PASS** — 4/4 checks, including against a live warehouse |
| `make lint` | `ruff check`, `black --check` (29 files) and `sqlfluff lint` all clean — sqlfluff with zero rule exclusions |
| `make demo` | 46 change events, 7 batches, reconciliation `delta=0` on all 6 tables |
| `make stop-the-line` | **PASS** — halted, stayed halted across restart, resumed on acknowledgement |
| `make crash-recovery` | **PASS** — no loss, no duplicates, idempotent replay |
| `dbt parse` then bare `dbt test` (the CI path) | **128 PASS** — bootstrapped from the committed lake fixture |

`make checks` runs the full set in CI's order, ~2 minutes cold.

**Test coverage by category** (89 cases): source constraints 13 · CDC capture 9 · replay and recovery 8 · schema gate 21 · warehouse correctness 11 · time travel 8 · validation parity 7 · catalog 9. Breakdown in `docs/07-ci-and-testing.md`.

**One CI note worth reading.** The `validate-models` job runs `dbt parse` then `dbt test` with no `dbt run` between them. The easy fix — deleting `dbt_project.yml` so the job prints "skipping" — would make the check vacuously green. Instead an `on-run-start` hook materialises the warehouse from a committed 46-record lake extract using **exactly the macros the models use** (model SQL lives in `macros/warehouse.sql`; the model files are one-line wrappers), and only for `dbt test`. So that check exercises real model SQL and real assertions. Explained in `docs/07-ci-and-testing.md`.

---

## Known Limitations / Next Steps

Ordered by how much they would matter in production.

1. **No retention or erasure policy.** The lake grows without bound and holds PII in before/after images, so "keep everything forever" collides with erasure obligations. The shape of the answer — bounded full history, then compaction to periodic snapshots plus tombstones, with erasure as an explicit audited exception to immutability — is sketched in `docs/04-time-travel.md` but not built.
2. **Trigger-based capture is blind to new columns.** A compatible column addition raises a WARN telling the operator to regenerate the capture triggers; automating that regeneration is the obvious next step. A WAL backend removes the problem entirely.
3. **A failing assertion fails the whole build.** No row-level quarantine, so bad data blocks good data from being refreshed. The right design routes offending rows aside for the non-critical assertions.
4. **The halt is pipeline-wide, not table-wide.** Deliberate — cross-entity assertions make a partially fresh warehouse harder to reason about than a uniformly stale one — but at a larger table count that trade flips. The gate is already per-table internally, so the change would be local.
5. **Freshness is measured source-to-lake, not source-to-warehouse.** The warehouse refresh duration is logged but not folded into the SLO.
6. **Composite unique constraints and multi-column foreign keys are skipped by the parity checker.** There are none in this model; the checker should be extended at the same time as the first one is introduced.
7. **Single process, micro-batch.** Fine to roughly 5k changes/second. The `(table_name, event_id)` merge key already makes partitioned parallel consumers safe, so scaling is partitioning on `table_name + hash(pk_hash)`, not a redesign.
8. **Local stack.** SQLite instead of Postgres, NDJSON instead of Delta/Iceberg, DuckDB instead of a distributed warehouse. Reasoning and thresholds in ADR-0002; the dbt models are portable.

---

## Responsible AI Usage

**Did I use AI tools?
Yes — Claude, to better understand the approach to select, for drafting implementation code, the SQL models and the documentation.

**Where it helped.
Boilerplate volume: the 18 capture triggers, the six near-identical model wrappers, the catalog YAML, the first drafts of the docs. It was also useful as a sounding board on the compatibility matrix.

**What I reviewed, validated and corrected.
The design decisions are mine and are recorded as ADRs. Specific things I caught and fixed while validating generated output — every one of them a real defect that the first draft had:

- **Cascaded deletes were silently not captured.
SQLite does not fire delete triggers for `ON DELETE CASCADE` rows unless `PRAGMA recursive_triggers` is on. The pipeline looked correct and was quietly dropping child deletes. Now covered by `test_cascaded_child_deletes_are_captured`.
- **Enum domain parsing picked the wrong `CHECK`.** `payment.status` has both an inline domain check and a later conditional check mentioning the same column; the parser took the last match and published a two-value domain instead of four. Fixed to first-match with the convention documented.
- **`INTEGER PRIMARY KEY` was published as nullable.** SQLite reports `notnull = 0` for a rowid alias, which would have dropped the not-null assertion on every surrogate key downstream.
- **A flaky time-travel test surfaced a real design gap.** Restoring "one millisecond before the tombstone" intermittently returned the wrong version because two changes had committed in the same millisecond. Rather than widen the tolerance I added `as_of_version` and documented that timestamps cannot separate same-millisecond changes — see ADR-0003.
- **A read-only DuckDB connection conflicted with dbt's own handle** once a build and a query shared a process.
- **The dbt profile's default database path pointed at a directory that does not exist on a fresh checkout.** DuckDB creates the file but not its parent, so the `validate-models` CI job would have failed before it started. Caught by running the CI sequence on a wiped tree rather than on my working copy.
- **The `change_type` schema mutation left a type-dependent `CHECK` in place** and failed on seeded data, which turned out to be a useful illustration of why the change is breaking in the first place.

I also rejected suggestions I disagreed with: generating the capture triggers from the contracts (which would have made the schema gate vacuous — the contract and the source would have been the same artefact), and dropping `dbt_project.yml` to make the `validate-models` CI job pass trivially.

---

## Author Checklist

- [x] Linting passes — `ruff check`, `black --check` and `sqlfluff lint` clean
- [x] Tests pass — 89 pytest cases
- [x] Model validation/tests pass — `dbt build`: 14 models, 127 assertions
- [x] Schema compatibility checks pass — `scripts/check_schema_contracts.py`
- [x] Data quality validations pass — 87/87 platform checks
- [x] Catalog metadata validation passes — `scripts/validate_catalog.py`
- [x] README tested from a clean state — `pip install -r requirements.txt && make demo`
- [x] End-to-end CDC flow validated locally — plus `make stop-the-line` and `make crash-recovery`
- [x] AI usage disclosed above