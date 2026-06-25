# Architecture

The pipeline is six layers stacked in a single process. Each layer has a
narrow responsibility and a typed contract with the layer above and below
it. The contracts are what make the system reliable under replay,
restart, and schema drift.

```
                         ┌──────────────────────────────────────┐
                         │ 6. Catalog (catalog/catalog.json)    │
                         └──────────────────────────────────────┘
                                          ▲ publishes
   ┌──────────────────┐                   │
   │ 5. Quality /     │ asserts ─────────►│ wh_*  +  wh_*_history
   │    Validation    │                   │
   │ (run_data_       │
   │  quality_checks) │
   └──────────────────┘                   ▲ merges (idempotent)
                                          │
                                ┌────────────────────┐
                                │ 4. Warehouse       │  current-state + SCD2
                                │ (pipeline/warehouse│  state_at(ts), restore
                                └────────────────────┘
                                          ▲ replayable from
                                          │
                                ┌────────────────────┐
                                │ 3. Lake            │  append-only event log
                                │ (pipeline/lake)    │  lake_cdc_events
                                └────────────────────┘
                                          ▲ writes (sequence-deduped)
                                          │
                ┌──────────────────────────────────────────────────┐
                │ 2. Ingestion / CDC layer                         │
                │   CDCCapture  →  Orchestrator (run_tick)         │
                │   schema gate (SchemaDetector) — stop-the-line   │
                │   per-stage checkpoints (pipeline_checkpoints)   │
                └──────────────────────────────────────────────────┘
                                          ▲ observes via contracts
                                          │
                                ┌────────────────────┐
                                │ 1. Source          │
                                │ DDL + indexes +    │
                                │ CHECK constraints  │
                                └────────────────────┘
```

---

## 1. Source layer

**File:** `source/models.py`, `source/contracts.py`, `source/seed.py`

Six tables in dependency order:

| Table              | Strong / Weak | Parent                          |
|--------------------|---------------|---------------------------------|
| `customers`        | strong        | —                               |
| `wallets`          | strong        | references customers            |
| `transfers`        | strong        | references wallets, pay_methods |
| `payment_methods`  | weak          | customer                        |
| `transfer_events`  | weak          | transfer                        |
| `ledger_entries`   | weak          | wallet                          |

A table is **weak** when its lifecycle is bound to a parent. A
payment_method has no business meaning if its customer is removed; a
ledger_entry has no meaning without its wallet. Strong entities can be
queried, ranked, and reasoned about on their own.

Every table carries `created_at` / `updated_at`, primary keys are
non-null VARCHAR identifiers, and enums are enforced via `CHECK`
constraints. Indexes mirror real CDC and lookup patterns:

- `updated_at` on `customers`, `wallets`, `transfers` — incremental
  extraction by timestamp
- foreign-key indexes — referential lookup + cascading delete scans
- `(wallet_id, occurred_at)` on `ledger_entries` — audit-by-wallet
- `status` indexes on `customers`, `transfers` — filtered scans

**Contracts** (`source/contracts.py`) are a frozen, versioned mirror of
this DDL. The schema detector and the data-quality layer both read from
contracts so a single change in `contracts.py` propagates everywhere.

---

## 2. CDC / Ingestion layer

**Files:** `pipeline/cdc.py`, `pipeline/orchestrator.py`,
`pipeline/checkpoint.py`

`CDCCapture` is the in-process stand-in for a Debezium-like log
consumer. Each `CDCRecord` carries:

| Field          | Why                                                |
|----------------|----------------------------------------------------|
| `operation`    | `insert` / `update` / `delete`                     |
| `table`        | source table name                                  |
| `primary_key`  | natural key value                                  |
| `before`       | pre-image (required for update / delete)           |
| `after`        | post-image (required for insert / update)          |
| `captured_at`  | wall clock at capture (UTC, defaults to `now`)     |
| `sequence`     | monotonic offset (Postgres LSN / Kafka offset)     |

The `sequence` is the key to everything downstream: it makes the lake
deduplicate trivially (`sequence` is its primary key) and makes the
warehouse merge idempotent and out-of-order safe (each warehouse row
records the last `_cdc_seq` that touched it).

`CDCCapture` can optionally hold a `SchemaDetector` and refuse writes
when the source schema is breaking — see `test_capture_with_strict_schema_refuses_writes_on_drop`.

**`run_tick(conn, capture, *, detector, fail_on_drift=True)`** is the
single orchestration entry point:

```
1. bootstrap (idempotent CREATE TABLE IF NOT EXISTS) for lake / wh / checkpoints
2. if detector reports BREAKING drift → return aborted=true, do not move data
3. read records since min(checkpoint_lake, checkpoint_warehouse) from capture
4. write records with sequence > checkpoint_lake to the lake
5. advance checkpoint_lake
6. merge records with sequence > checkpoint_warehouse into the warehouse
7. advance checkpoint_warehouse
```

Each step is independently safe to retry. If step 6 fails, step 4-5 may
have committed; the next tick sees `checkpoint_lake > checkpoint_warehouse`
and re-merges from the lake / capture. The warehouse merge silently
no-ops on already-applied sequences.

**Checkpoints** (`pipeline_checkpoints`) are an ordinary table in the
same DuckDB database as the warehouse. In production, this would be a
Kafka consumer group offset committed inside the same transaction as
the warehouse batch.

---

## 3. Lake layer

**File:** `pipeline/lake.py`

A single table:

```sql
CREATE TABLE lake_cdc_events (
    sequence     INTEGER PRIMARY KEY,    -- dedup
    operation    VARCHAR NOT NULL,
    table_name   VARCHAR NOT NULL,
    primary_key  VARCHAR NOT NULL,
    before_image VARCHAR,                -- JSON
    after_image  VARCHAR,                -- JSON
    captured_at  TIMESTAMP NOT NULL
)
```

**Append-only.** `append_to_lake` uses `ON CONFLICT (sequence) DO NOTHING`,
so replay is a no-op. `replay_records(conn, table=None, until_sequence=None)`
returns events in `sequence` order and is what the warehouse would replay
from after disaster recovery.

The before/after row images are JSON. A custom serializer handles
`Decimal`, `datetime`, and `date` so monetary values round-trip exactly
(`Decimal("75.0000")` stays a `Decimal` after `json.dumps` →
`json.loads`).

**Production analogue:** an Iceberg/Delta bronze table partitioned by
`(table_name, captured_at::date)`. The PK on `sequence` becomes a Delta
Lake `MERGE` key.

---

## 4. Warehouse layer

**File:** `pipeline/warehouse.py`

For each source table `T` we materialize **two** tables:

### `wh_T` — current state

The current-state row carries `_cdc_seq` (the sequence of the last
event applied) and `_deleted` (set true on delete; the row is kept so
analytics over deleted entities still works).

### `wh_T_history` — SCD2 history

Each version has:

| Column        | Meaning                                           |
|---------------|---------------------------------------------------|
| `_cdc_seq`    | sequence that produced this version               |
| `_valid_from` | inclusive start of validity (= `captured_at`)     |
| `_valid_to`   | exclusive end of validity, NULL for current row   |
| `_is_current` | redundant flag, indexes well                      |
| `_op`         | `insert` / `update` / `delete`                    |
| PK            | `(business_key, _valid_from)`                     |

### Idempotent merge

```python
existing = SELECT _cdc_seq FROM wh_T WHERE pk = ?
if existing and existing._cdc_seq >= record.sequence:
    return  # already applied, or out-of-order — no-op
```

This single check makes the merge safe under:

- **duplicate events** (same sequence ≤ existing → no-op)
- **out-of-order arrival** (smaller sequence → no-op; tested in `test_older_event_does_not_clobber_newer_state`)
- **retry after partial failure** (the merge is on a per-row basis, so re-running yields the same final state)

### Time travel

```python
state_at(conn, table, ts) -> list[dict]
```

scans `wh_T_history` for rows where
`_valid_from <= ts < COALESCE(_valid_to, '9999-12-31')` and
`_op != 'delete'`. The result is "what `SELECT * FROM T` would have
returned at `ts`".

`restore_current_state_to(conn, table, ts)` truncates `wh_T` and reloads
it from `state_at(table, ts)`. History is **never** modified — restore
is a current-state operation only.

---

## 5. Quality / Validation layer

**File:** `scripts/run_data_quality_checks.py`

Rules are **derived** from `source/contracts.py`, so adding a new
NOT NULL column or a new enum domain in `contracts.py` automatically
generates the corresponding warehouse rule. There is no second source
of truth.

### System rules (auto-generated from contracts)

- `sys.pk_unique.<table>` — PK has no duplicates
- `sys.not_null.<table>.<col>` — for every NOT NULL column in the contract
- `sys.enum.<table>.<col>` — for every column with an enum domain
- `sys.fk.<table>.<col>` — referential integrity, ignoring `_deleted`

### Business rules (explicitly defined)

- `biz.wallet.balance_non_negative`
- `biz.transfer.amount_positive`
- `biz.transfer.no_self_transfer`
- `biz.transfer.settled_at_after_created`
- `biz.transfer.settled_implies_settled_at`
- `biz.transfer.failed_implies_reason`
- `biz.ledger.signed_amount_consistent`
- `biz.ledger.balance_after_non_negative`
- `biz.ledger.reconciles_to_wallet_balance` — `SUM(signed_amount) == balance` per wallet
- `biz.transfer_event.allowed_transition`

`run_checks` returns a list of `(rule, count_of_failing_rows)` pairs.
The CI script exits non-zero if the list is non-empty.

`tests/test_data_quality.py` proves both directions: the seeded
warehouse passes all rules **and** each rule actually fires when its
invariant is violated.

---

## 6. Catalog layer

**File:** `catalog/catalog.json`

13 datasets — 1 lake event log + 6 warehouse current-state + 6
warehouse SCD2 history. Each carries:

- `name`, `layer`, `type` (controlled vocabulary: `lake` / `warehouse`,
  `append-only-change-log` / `current-state` / `scd2-history`)
- `description`, `owner`, `consumers`
- `update_cadence`, `sla_class` (`tier-1` / `tier-2` / `tier-3`)
- `tags`, `primary_key`, `schema` or `schema_extension`

`scripts/validate_catalog.py` enforces:

- every expected dataset is present
- every required field is populated
- the layer/type pairing is consistent
- `tests/test_catalog.py` adds 14 more assertions including controlled
  vocabularies, money-critical SLA tier, and time-travel tagging on
  history datasets

In production this catalog would publish to DataHub / Atlan / Unity
Catalog. The JSON shape is deliberately close to OpenAPI dataset
declarations so the migration is a serializer swap.

---

## Failure modes and how the architecture handles them

| Failure                              | Handled by                                                  |
|--------------------------------------|-------------------------------------------------------------|
| Duplicate CDC event                  | `lake.sequence` PK + warehouse `_cdc_seq` guard             |
| Out-of-order arrival                 | warehouse `_cdc_seq` guard rejects lower sequences          |
| Retry after partial failure          | each tick step is idempotent; checkpoints advance per stage |
| Restart with progress checkpointed   | `pipeline_checkpoints` + `min(lake, wh)` floor in `run_tick`|
| Incompatible schema change           | `SchemaDetector` BREAKING + orchestrator stops the line     |
| Source delete                        | warehouse soft-deletes (`_deleted=true`); SCD2 closes row   |
| Late-arriving update with smaller seq| warehouse merge guard rejects                               |
| Need to recover historical state     | `state_at(ts)` reconstructs from SCD2; `restore_current_state_to` |
| Ledger / wallet drift                | `biz.ledger.reconciles_to_wallet_balance` rule fails CI     |
