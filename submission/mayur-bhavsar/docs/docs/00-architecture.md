# Architecture

## The four properties everything rests on

Almost every design decision in this repository follows from one of these.
They are stated first because they are what the tests actually assert.

### 1. The warehouse is a pure function of the lake

No model reads the source database. Every warehouse relation is derived from
`lake.cdc_events` and nothing else, and ordering inside those models is by
`event_id` — never arrival time, never wall clock.

Consequences: a replay from offset zero is a no-op; a rebuild after data loss is
self-healing; out-of-order arrival cannot change the result; and a duplicate
delivery is absorbed rather than double-counted.

Asserted by `test_the_warehouse_never_reads_the_source`,
`test_out_of_order_arrival_produces_the_same_warehouse`,
`test_full_replay_from_zero_reproduces_the_warehouse`.

### 2. The checkpoint is derived, not stored

There is no offset file beside the data that could disagree with it. The
checkpoint is `max(checkpoint_after)` over the lake manifest, and the manifest
line is only appended after the data files are durable.

Consequence: the failure window between "data written" and "progress recorded"
produces a **duplicate**, never a **loss** — and duplicates are absorbed by
property 1.

### 3. The lake schema does not change when the source schema changes

`pk`, `before` and `after` are stored as JSON text, so the physical lake record
has the same eleven columns forever. A new source column produces different
JSON, not a different file layout.

Consequence: schema drift can corrupt neither the change log nor the ability to
replay history recorded under an older contract. Typing happens once,
downstream, against the published contract — where it can be checked.

### 4. Incompatible drift stops the line and stays stopped

The gate runs before a batch reads a single row. A breaking difference between
the live schema and the published contract transitions a *persisted* state
machine to `HALTED`, and the runner refuses to start until a human
acknowledges it.

Consequence: a scheduler cannot resume by simply running again at 03:00.

---

## Layers

```
┌─ Source ──────────────────────────────────────────────────────────────────┐
│ sql/001_source_schema.sql   6 tables, keys, FKs, CHECK domains, indexes    │
│ sql/002_cdc_triggers.sql    18 AFTER triggers -> cdc_events                │
│ cdclake/source.py           connection pragmas, seed, workload, mutations  │
└───────────────────────────────────────────────────────────────────────────┘
                │  introspection                       ▲  nothing here reads
                ▼                                      │  a contract file
┌─ Contracts ───────────────────────────────────────────────────────────────┐
│ contracts/*.json            published belief: columns, types, nullability, │
│                             enum domains, keys, FKs, uniques, indexes      │
│ cdclake/contracts.py        introspection + the compatibility policy       │
└───────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─ Ingestion ───────────────────────────────────────────────────────────────┐
│ cdclake/gate.py             schema gate + RUNNING/HALTED state machine     │
│ cdclake/ingest.py           CdcSource protocol, TriggerAuditSource, runner │
└───────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─ Lake ────────────────────────────────────────────────────────────────────┐
│ cdclake/lake.py             immutable NDJSON files + manifest + checkpoint │
│ lake/cdc_events/table_name=*/ingest_date=*/batch_*.ndjson                  │
└───────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─ Warehouse ───────────────────────────────────────────────────────────────┐
│ models/staging/stg_cdc_events        typed, deduplicated                   │
│ models/history/dim_*_history         SCD2, one row per version             │
│ models/current/*_current             open interval, tombstones excluded    │
│ models/marts/mart_order_financials   cross-entity rollup                   │
│ cdclake/warehouse.py                 dbt invocation + as_of / as_of_version│
└───────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─ Quality ─────────────────────────────────────────────────────────────────┐
│ models/schema.yml           127 generic assertions (parity with contracts) │
│ dbt_tests/*.sql             12 business + structural assertions            │
│ cdclake/quality.py          reconciliation, completeness, durability,      │
│                             freshness, and the parity checker itself       │
└───────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─ Catalog ─────────────────────────────────────────────────────────────────┐
│ catalog/datasets.yml        15 datasets: owner, consumers, cadence,        │
│                             lineage, PII, access path, schema             │
│ cdclake/catalog.py          validated against the live warehouse          │
└───────────────────────────────────────────────────────────────────────────┘
```

`cdclake/pipeline.py` is the composition root — the only place the layers are
wired together, so the CLI, the CI scripts and the tests all assemble the same
platform.

---

## Why the source is hand-written and the warehouse spec is generated

`sql/002_cdc_triggers.sql` is 18 hand-written triggers. It would have been
shorter to generate them from `contracts/*.json`.

That would have been wrong. The source system belongs to another team. If this
pipeline generated its own source, the schema gate could never detect drift,
because there would be nothing to drift *from* — the contract and the source
would be the same artefact wearing two hats. The gate is only meaningful
because the two are independent and are compared.

The opposite reasoning applies downstream. `macros/_entities.sql` **is**
generated from the contracts, because there the contract is upstream of the
warehouse and a hand-maintained copy would silently rot.
`scripts/generate_dbt_entities.py --check` fails CI if the two disagree.

---

## Simplifications, and what would change in production

| Here | In production | Why it is safe to differ |
|---|---|---|
| SQLite source | Postgres | The DDL, constraints and indexes are the modelling exercise; the engine is not. Postgres gives exact `NUMERIC` and native enums, both of which this design already accounts for. |
| Trigger-based capture | Debezium on the WAL | Both sit behind `CdcSource`. Triggers cost write amplification on the OLTP path and cannot see a column they were not regenerated for; the WAL has neither problem. [ADR-0001](adr/0001-cdc-capture-mechanism.md) |
| `event_id` as the offset | Postgres LSN | Identical semantics *here* because SQLite serialises writers, so the sequence is assigned in commit order. That is **not** true of a sequence-backed audit table on Postgres, which is the real reason production uses the LSN. [docs/02](02-cdc-contract.md#why-event_id-is-a-safe-offset-here-and-would-not-be-on-postgres) |
| NDJSON files + manifest | Delta or Iceberg | The manifest is a hand-rolled version of what a table format's transaction log provides. Reviewable as text, and it makes the atomicity argument explicit rather than delegated. [ADR-0002](adr/0002-local-stack.md) |
| DuckDB warehouse | Snowflake / BigQuery / Databricks SQL | The dbt models are portable; only the profile changes. |
| Micro-batch, single process | Partitioned consumers | Fine to roughly 5k changes/second. The `(table_name, event_id)` merge key already makes parallel consumers safe; partition by `table_name + hash(pk_hash)`. |
| Money as SQLite `DECIMAL(12,2)` | Postgres `NUMERIC(12,2)` | SQLite applies NUMERIC affinity and stores a float. Every monetary CHECK is written with `ROUND(x, 2)` and the warehouse casts once to `DECIMAL(18,2)`, so warehouse arithmetic is exact. [ADR-0002](adr/0002-local-stack.md) |
| Alerts to a file | PagerDuty / Slack webhook | A file is assertable, which is why the tests can prove a warning was actually surfaced rather than assuming a log line was written. |

## What is deliberately not built

- **No automatic migration of breaking changes.** The assignment does not ask
  for it, and adopting a breaking change is a decision about the *meaning* of
  data. `republish-contracts` is a deliberate operator command, never something
  the runner does on its own.
- **No streaming.** Freshness is a stated 60-second SLO measured every batch,
  not a claim.
- **No IAM, no UI, no orchestration.** Out of scope, and adding them would not
  make the correctness story stronger.
- **Trigger regeneration after a compatible column addition.** The gate raises
  a WARN saying the triggers must be regenerated before the new column reaches
  the lake; automating it is the obvious next step and is called out in the PR
  as a known limitation.
