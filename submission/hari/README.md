# CDC Lakehouse — E-commerce Orders

| Document | Contents |
|---|---|
| **README.md** (this file) | Problem, architecture, design decisions, how to run |
| [SCHEMA.md](./SCHEMA.md) | Source ER diagram, tables, keys, indexes, lifecycles, invariants |
| [TEST_PLAN.md](./TEST_PLAN.md) | Test approach, harness, scenario matrix, requirement coverage |
| [OPERATIONS.md](./OPERATIONS.md) | Configuration, statuses, runbook, recovery matrix, restore procedures |

---

## 1. Problem Statement

An online store sells products to registered users. A user places an order for one or
more products and pays with the **in-app wallet, a card or UPI** — or a mix of them.
Placing an order reserves inventory; a delivered item can be partly or fully refunded,
and the money goes back to the method it came from. All of this lives in a
transactional database that the application owns.

Other teams need this data — finance to reconcile payments and refunds, analytics to
track sales and stock, audit to see who changed what and when. They cannot query the
production database, and a nightly copy is not enough:

- **Data changes constantly.** An order moves through several statuses in minutes; a
  periodic snapshot misses the intermediate states and any deleted rows.
- **History matters.** Disputes and audits ask what an order or wallet looked like at a
  specific moment, and a bad load must be recoverable.
- **The source schema can change without notice.** The application team makes no
  backward-compatibility promise; silently ingesting a renamed or retyped column would
  corrupt every downstream number.
- **Downstream data must be as trustworthy as the source.** The rules the application
  enforces — valid statuses, totals that add up, balances that match the ledger — must
  still hold after the data has moved.

This project builds the pipeline that solves this: it captures every change from the
source, keeps the full change history in a **lake**, maintains an up-to-date and
time-travelable **warehouse**, stops safely when the source schema breaks its
contract, and publishes both layers in a **catalog**.

## 2. Guarantees

| # | The platform guarantees that… | Section |
|---|---|---|
| G1 | every insert, update and delete in the source is captured | §4 |
| G2 | the lake keeps every change, unmodified | §5 |
| G3 | the warehouse reflects the latest state within one pipeline cycle | §6, §10 |
| G4 | retries, replays and restarts never lose or duplicate a change | §4 |
| G5 | an incompatible schema change stops ingestion with a clear alert | §7 |
| G6 | any past point in time can be read back, restored to the source, or rebuilt in the warehouse | §6 |
| G7 | every source rule is re-checked in the warehouse | §8 |
| G8 | lake and warehouse datasets are discoverable with useful metadata | §9 |
| G9 | runs are reproducible and every failure has a defined recovery path | [OPERATIONS.md](./OPERATIONS.md) |

**Out of scope:** real Postgres/Kafka infrastructure, automatic migration of breaking
schema changes, access control, dashboards, multiple currencies or stock locations.

---

## 3. Architecture

    ┌──────────── SQLite: source.db (the application database) ────────────┐
    │ 9 business tables  ──AFTER INSERT/UPDATE/DELETE triggers──▶ change_log │
    └───────────────────────────────┬──────────────────────────────────────┘
                                    │ pull: seq > checkpoint
    ┌─────────── pipeline (Python) ─▼──────────────────────────────────────┐
    │ run_once(): status → schema contract check ──✗──▶ STOPPED + alert     │
    │             → read whole source transactions                         │
    │             → one DuckDB transaction: lake + warehouse + quality +   │
    │               checkpoint                                             │
    │ run_forever(): adaptive polling loop around run_once()               │
    └───────────────────────────────┬──────────────────────────────────────┘
    ┌──────────── DuckDB: platform.duckdb ─▼───────────────────────────────┐
    │ lake.cdc_events            every change, append-only                 │
    │ warehouse.<table>          latest state (view over <table>_current)  │
    │ warehouse.<table>_history  every version, for time travel            │
    │ cdc.*  checkpoint · pipeline_status · alerts · schema_registry ·     │
    │        pipeline_lock · migrations                                    │
    │ quality.*  check_results · quarantine                                │
    └──────────────────────────────────────────────────────────────────────┘
    catalog/catalog.json — published metadata for lake and warehouse datasets

| Layer | Engine | Responsibility | Code |
|---|---|---|---|
| Source | SQLite | System of record; enforces constraints; emits changes via triggers | `source/` |
| CDC | Python | Contract check, read from checkpoint, batching, stop-the-line | `pipeline/cdc/` |
| Lake | DuckDB `lake` | Complete, immutable change history for replay and audit | `pipeline/lake/` |
| Warehouse | DuckDB `warehouse` | Latest state and versioned history; restore | `pipeline/warehouse/` |
| Quality | DuckDB `quality` | Validation parity and quarantine | `pipeline/quality/` |
| Catalog | JSON | Discovery and metadata | `catalog/` |

SQLite plays the transactional database (transactions, constraints, triggers); DuckDB
plays the analytical platform. The pipeline is the only bridge between them.

## 4. Source and Change Capture

The source has 9 tables — users, products, inventory, wallets, wallet_transactions,
orders, order_items, payments, refunds. Full design: **[SCHEMA.md](./SCHEMA.md)**.

**Capture.** `AFTER INSERT/UPDATE/DELETE` triggers on every table write a row to
`change_log` in the **same transaction** as the business write, so a change can never
commit without its event. Triggers are generated from the schema contract, so every
contracted column is captured, and each trigger stamps the contract version it was
generated from. The application code knows nothing about CDC.

**Transaction id.** SQLite has no transaction id, so write helpers set one in a
one-row `_txn_context` table at the start of each transaction and triggers copy it into
every event. A direct write that bypasses the helpers gets a generated id. Primary keys
are immutable (enforced by trigger), so an event's `pk` always identifies the same row.
Details: [SCHEMA.md §8](./SCHEMA.md#8-cdc-support-tables-in-the-source).

**Change event**

| Field | Meaning |
|---|---|
| `seq` | Global, strictly increasing. SQLite serialises writers, so `seq` order = commit order |
| `txn_id` | Source transaction id, shared by all events of one business operation |
| `schema_version` | Contract version of the trigger that captured the event |
| `table_name`, `op` | `insert` \| `update` \| `delete` |
| `pk` | Primary key as JSON (supports composite keys) |
| `before`, `after` | Full row images as JSON (`before` empty on insert, `after` empty on delete) |
| `changed_at` | Source commit time |

**Batching.** Read `seq > checkpoint` in order, then extend the batch to the end of the
last transaction, so the warehouse never shows half an order.

**Delivery guarantees — at-least-once delivery, exactly-once effect**

| Situation | Handling |
|---|---|
| Crash mid-batch | Lake, warehouse, quality results and checkpoint commit in **one** DuckDB transaction; a crash rolls back and the batch is re-read |
| Duplicate event | Lake skips a `seq` it already holds; warehouse applies an event only if `seq > row._last_seq` |
| Late / out-of-order event | Same sequence guard — older events never overwrite newer state, but are still recorded |
| Delete | Lake keeps the `before` image; the current row becomes a tombstone; history closes the version |
| Restart | Resume from `cdc.checkpoint`, stored in the same DuckDB file as the lake and warehouse so all three commit together |

**Initial load.** Triggers are created in the same migration as the tables, so
`change_log` covers every row from the first write. For a source that already holds
data, `bootstrap()` installs the triggers, records `S0 = max(seq)`, reads one consistent
snapshot of every table and loads it as `snapshot` events, then streams from `S0`. Rows
changed during the snapshot are re-applied by their later events — harmless, because
events carry full row images and the sequence guard keeps the newest.

## 5. Lake

`lake.cdc_events` holds every `change_log` field plus `batch_id`, `ingested_at` and
`contract_version`. It is unique on `seq` and only ever appended to. It is the source of
truth for audit, replay and warehouse rebuilds. Raw PII lives here, so access is restricted.

## 6. Warehouse

- **Types:** paise → `DECIMAL(18,2)` rupees, text → `TIMESTAMP`; `email` and `phone` hashed.
- **Current state:** `<table>_current` = source columns + `_last_seq`, `_last_txn_id`,
  `_is_deleted`, `_updated_at`, and `_schema_version` (contract version of the last event). Tombstones stay so late events cannot resurrect deleted
  rows. Consumers query the view `warehouse.<table>`, which hides them.
- **History (SCD2):** `<table>_history` = source columns + `valid_from`, `valid_to`,
  `is_current`, `op`, `seq`, `txn_id`, `_schema_version`. No primary-key constraints are declared on warehouse tables; uniqueness is enforced by quality rule S1. Every event closes the previous version and opens a new one.
- **Point in time** is source commit time (`changed_at`). Because a batch holds whole
  transactions in commit order, every table is consistent with the others at any `ts`.

| Need | Tool | What it does |
|---|---|---|
| See the past | `as_of(ts)` | Read every table as it was at `ts`; read-only |
| Undo a bad change in the source | `export_as_of(table, keys, ts)` | Produces the rows as they were at `ts`; they are written back to the **source** through the normal helpers, so the restore flows through CDC and is itself audited |
| Repair a corrupted warehouse | `rebuild()` / `rebuild_to(seq \| ts)` | Rebuilds current and history from the lake (all, or up to a point); `rebuild()` requires the pipeline to be `PAUSED` — for pipeline bugs, not for undoing source data |

Rebuilding from the whole lake must equal the incrementally built warehouse — this is
tested. Step-by-step procedures: [OPERATIONS.md §6](./OPERATIONS.md#6-restore-procedures).

## 7. Schema Contract and Safe Stop

`contracts/source_contract.yml` (versioned) declares each table's columns with type,
nullability and enum domain, its primary and foreign keys, and its CDC trigger.

Two checks, both fail closed:

- **Live schema, before every batch** — `PRAGMA table_info`, `foreign_key_list`, CHECK
  domains and the trigger list are compared with the active contract. This matters
  because SQLite rewrites triggers on a column rename, so a rename never shows up in
  event payloads.
- **Every event** — its `schema_version` must be a registered contract version, and its
  values must match that version's types, nullability and domains (this catches, for
  example, a new enum value written before any DDL check runs).

**Exact boundaries.** SQLite has no DDL triggers, so the live check can only notice a
change at the next batch — valid events captured before the change are held too (safe,
not lossy). The `schema_version` stamp is what makes the boundary exact afterwards: once
contract N+1 is approved and triggers are regenerated, a backlog mixing N and N+1 events
is validated event by event against the version each was captured under.

| Change | Verdict |
|---|---|
| New nullable column, new table | Warn — ignored until the contract is updated |
| Dropped or renamed column | **Stop** |
| Type change | **Stop** |
| Nullability change (either way) | **Stop** |
| Enum value added or removed | **Stop** |
| Primary or foreign key change | **Stop** |
| Dropped table or missing CDC trigger | **Stop** |

**On stop:** raise `SchemaContractViolation`, write nothing, set `cdc.pipeline_status` to
`STOPPED` with the diff, add a row to `cdc.alerts`, log an error; scripts exit non-zero and
`run_forever()` exits. Changes keep accumulating in `change_log`, so nothing is lost.

**Approving a change.** Each contract version is stored in `cdc.schema_registry`
(version, contract, diff from previous, approved at). The warehouse migration that
accompanies it follows one rule — **history is never rewritten**: a new column is added
(NULL for older versions), a dropped column stays in the warehouse and is NULL from then
on, a type change becomes a new column. Then `resume()` continues from the checkpoint.
Runbook: [OPERATIONS.md §4.1](./OPERATIONS.md#41-stopped--incompatible-schema-change).

## 8. Validation Parity

Every source invariant (S1–S14 in [SCHEMA.md §6](./SCHEMA.md#6-invariants)) has a
warehouse check, run inside the batch transaction and recorded in `quality.check_results`.

**One definition per rule.** Column-level rules (types, NOT NULL, domains, ranges) live
once in the contract, which generates both the SQLite CHECK constraints and the warehouse
checks. Cross-row rules live once in a rules registry (`pipeline/quality/rules.py`) keyed
by rule ID. A parity test fails if any rule ID in SCHEMA.md has no warehouse check, so the
two sides cannot drift apart silently.

| Source rule | Warehouse check | On failure |
|---|---|---|
| S1 keys, FKs, NOT NULL, enums | uniqueness, anti-join, null and domain queries | **Block** — roll back, status `FAILED` |
| S2–S4, S6, S7 row rules | row predicates | **Block** |
| S8 ledger append-only | no update/delete events on `wallet_transactions` | **Block** |
| S5 status transitions | consecutive history versions | Quarantine + alert |
| S9 order total = Σ lines | aggregate join | Quarantine + alert |
| S10 Σ successful payments = total | aggregate join | Quarantine + alert |
| S11 refund caps | aggregate join | Quarantine + alert |
| S12 wallet balance = ledger | aggregate join | Quarantine + alert |
| S13 reserved = open-order quantity | aggregate join | Quarantine + alert |
| S14 immutable keys; users/wallets never deleted | no key-changing updates; no deletes on those tables | **Block** |

*Block* = structural breakage; nothing downstream can be trusted — the batch rolls back,
the status becomes `FAILED`, and the checkpoint stays put. *Quarantine* = business
anomaly; the row is loaded, its key, rule and reason are copied to `quality.quarantine`,
and an alert is raised. Recovering from `FAILED`:
[OPERATIONS.md §4.2](./OPERATIONS.md#42-failed--blocking-quality-check).

## 9. Catalog

`catalog/catalog.json` lists every dataset with its layer, description, owner, intended
consumers, update cadence, PII flags, access level, schema and an example query.

| Dataset | Layer | Consumers | Access |
|---|---|---|---|
| `lake.cdc_events` | lake | platform, audit | restricted (raw PII) |
| `warehouse.<table>` | warehouse | analytics, finance, product | general |
| `warehouse.<table>_history` | warehouse | finance, audit | general |

Consumers find datasets in the catalog and query `platform.duckdb` read-only.
`scripts/validate_catalog.py` fails if a dataset is missing or its schema differs from
the live table.

## 10. Runtime

| Function | Use |
|---|---|
| `run_once(max_events)` | One atomic batch; returns events processed, lag and status. On a contract violation or blocking quality failure it records status and alert, then **raises** `SchemaContractViolation` / `QualityBlocked` |
| `run_until_caught_up()` | Drains the backlog — tests, demo, catch-up after an outage |
| `run_forever()` | Adaptive polling: no wait while there is a backlog, idle back-off 0.2 s → 5 s, exits on `STOPPED`, graceful shutdown |

Each cycle moves every committed change, so the warehouse is at most one cycle behind
the source. Every batch records two lag measures in `cdc.pipeline_status`:
`lag_events = max(change_log.seq) − checkpoint` and
`freshness = now − changed_at of the oldest unprocessed event`. Freshness above 60 s
raises a warning and above 300 s a critical alert (configurable).

Status (`RUNNING`, `PAUSED`, `STOPPED`, `FAILED`), single-instance locking, configuration
and the runbook are in [OPERATIONS.md](./OPERATIONS.md).

## 11. Testing

Tests run the real path — SQL writes to SQLite → triggers → pipeline → lake → warehouse —
in a fresh temp directory per test, with a fake clock, crash injection and event
injection. Full scenario matrix and requirement coverage: **[TEST_PLAN.md](./TEST_PLAN.md)**.

## 12. Repository Layout

    submission/hari/
    ├── README.md  SCHEMA.md  TEST_PLAN.md  OPERATIONS.md
    ├── config.yml                paths, contract_path, batch size, polling, freshness thresholds
    ├── demo.py conftest.py       walkthrough; puts the folder on sys.path for pytest
    ├── docs/                     schema.mmd (diagram source), schema.png
    ├── requirements.txt          pinned: duckdb, pyyaml, pytest
    ├── contracts/                source_contract.yml (versioned)
    ├── source/                   SQLite DDL, triggers, write helpers (the application)
    ├── pipeline/
    │   ├── cdc/                  reader, contract check, checkpoint, runner
    │   ├── lake/                 lake writer
    │   ├── warehouse/            current + history, as_of, rebuild_to
    │   └── quality/              rules registry, parity checks, quarantine
    ├── catalog/catalog.json
    ├── scripts/                  CI entry points
    ├── demo.py                   end-to-end walkthrough
    └── tests/

## 13. Getting Started

Python 3.11+ (SQLite ships with Python).

    cd submission/hari
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python demo.py                              # deterministic end-to-end walkthrough incl. time travel
    pytest -q
    python scripts/check_schema_contracts.py
    python scripts/run_data_quality_checks.py
    python scripts/validate_catalog.py

## 14. Assumptions and Limitations

- CDC is simulated with triggers; production would use log-based capture with no
  triggers on the application database.
- Single process and a single source database; micro-batch, not sub-second streaming.
- Cross-row rules (S9–S13) are enforced by the application, not the database — which is
  why the warehouse re-checks them.
- Point in time means source commit time; "what did the warehouse show at time X"
  (ingestion time) is answerable from the lake's `ingested_at`, but there is no API for it.
- PII in the immutable lake conflicts with deletion requests — see OPERATIONS.md §7.
- `change_log` is never pruned here; production would prune below the slowest consumer's checkpoint.
- Breaking schema changes are detected and stopped, not migrated automatically.

| Here | Production analogue |
|---|---|
| SQLite + triggers + `change_log` | Postgres logical replication → Debezium → Kafka |
| `cdc.checkpoint` | Kafka consumer offsets |
| DuckDB `lake` | Parquet / Iceberg on object storage, partitioned by table and date |
| DuckDB `warehouse` | Snowflake / BigQuery with MERGE jobs |
| `cdc.alerts` + `pipeline_status` | Alerting (PagerDuty / Slack) and an orchestrator |
| `catalog.json` | DataHub / Glue / Unity Catalog |
