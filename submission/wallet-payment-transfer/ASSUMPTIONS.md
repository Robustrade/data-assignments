# Assumptions, Tradeoffs, and Production Analogues

This is a **single-process simulation** of a CDC lakehouse, written
to be reasoned about rather than deployed. Every shortcut taken below
has a documented production analogue. The goal is to make the
correctness story easy to follow without losing the structural shape
of a real system.

---

## What is simulated and what would change in production

### CDC capture

**Simulated:** `pipeline/cdc.py` — `CDCCapture.insert / update / delete`
called explicitly from seed code or tests. Each call appends a
`CDCRecord` with a strictly increasing `sequence` to an in-memory log.

**Production analogue:** Postgres logical decoding via Debezium →
Kafka. Sequence numbers become Kafka offsets (which, conveniently,
share the same monotonic-by-partition guarantees we rely on). The
`CDCRecord` shape lines up 1-1 with Debezium's envelope (`op`,
`table`, `before`, `after`, `ts_ms`, `lsn`).

What does **not** change: the `_cdc_seq` guard in the warehouse merge,
the lake's PK on `sequence`, and the orchestrator's bootstrap →
schema-gate → lake → warehouse flow. Those are the stable contracts.

### Storage

**Simulated:** DuckDB in-memory (or a single file) for source, lake,
warehouse, and checkpoint store.

**Production analogue:**
- **Source** lives in Postgres / MySQL — DuckDB exposes the same
  `information_schema.columns` introspection used by `SchemaDetector`,
  so the detector is portable.
- **Lake** is an Iceberg / Delta / Hudi table on object storage,
  partitioned by `(table_name, captured_at::date)`. The lake's
  `sequence` PK becomes a Delta `MERGE` key.
- **Warehouse** is Snowflake / BigQuery / Redshift. The current-state
  + SCD2 pattern translates verbatim — `MERGE INTO ... USING ... ON
  pk WHEN MATCHED AND target._cdc_seq < source.sequence THEN UPDATE`.
- **Checkpoints** live in a small operational store, ideally
  committed inside the same warehouse transaction as the merge.

### Schema detection

**Simulated:** `SchemaDetector` introspects the live source DDL via
`information_schema.columns` and `duckdb_constraints()` and compares
against `source/contracts.py`.

**Production analogue:**
- Confluent / AWS Glue Schema Registry as the system of record for
  Avro / Protobuf schemas; Debezium publishes new schemas there
- A simple comparator (the same logic as our `SchemaDetector`) runs
  on schema-changed events, emits a Slack / PagerDuty alert, and
  flips a feature flag the orchestrator polls

The detector's rule set (column_dropped, type_changed,
nullability_tightened, primary_key_changed, non_nullable_column_added
=> BREAKING; column_added, enum_value_added => WARN) is identical to
what the production analogue would enforce.

---

## Simplifying decisions

### Single-process / no orchestrator framework

The orchestrator is `pipeline/orchestrator.py:run_tick`, called
directly from scripts and tests. There is no Airflow / Dagster
because that would obscure the data-flow. The full tick is one
function call (~70 lines) that you can read top-to-bottom. In
production this becomes a Dagster `@op` or an Airflow `PythonOperator`.

### JSON-encoded before/after images instead of Avro

`json.dumps` with a custom `default=` for `Decimal`, `date`, and
`datetime`. JSON is human-readable and lossless for our types and
trivially queryable in DuckDB. Production would use Avro/Protobuf
because of schema evolution support; the contract surface (`before`,
`after` carry the source row image) does not change.

### Soft deletes only — never hard deletes

`wh_<table>` rows are flagged with `_deleted=true` instead of being
removed. This:

- preserves the row for analytics on deleted entities
- keeps FK integrity in `wh_<table>` even after a parent is removed
- mirrors what most warehouses already do — a hard delete is rare in
  analytical models

The data-quality FK rule explicitly ignores `_deleted=true` rows on
both sides (see `tests/test_data_quality.py::test_deleted_rows_do_not_trigger_validation_failures`).

### `captured_at` is the SCD2 valid_from

We use the CDC capture timestamp as the SCD2 boundary, not a separate
"as_of" column. In production with millisecond ordering issues we
would use `(captured_at, sequence)` as a composite key, but `sequence`
alone already provides total ordering, so the simpler boundary is
fine for our scope.

### No transaction boundaries across stages

In DuckDB single-process we could `BEGIN ... COMMIT` across the lake
write and warehouse merge — but that hides the realistic case where
lake and warehouse are different systems. We instead lean on the
**idempotent merge** + **per-stage checkpoints** to make a partial
failure recoverable without distributed transactions.

### Sequence is generated in `CDCCapture`

In production the sequence is the Postgres LSN (or Kafka offset). We
issue our own monotonic counter in `CDCCapture.latest_sequence + 1`
because there is no real WAL to read. The shape is identical.

### One DuckDB connection per test

Tests use `:memory:` connections, so they're hermetic. There is no
shared state between tests. The `seeded` fixture builds the entire
seed in-process per test, which is < 100ms.

### No partitioning on the lake table

Production would partition by `(table_name, captured_at::date)`. We
skipped this because DuckDB plans queries fine without it on our scale
and partitioning would obscure the model. The catalog entry already
documents the production partition.

---

## Things explicitly NOT solved

- **Automatic migration of breaking schema changes.** The assignment
  states this is out of scope. We *detect* and *stop* — we do not
  attempt to back-fill or reshape.
- **Deduplication on the source side** — `(table, primary_key, sequence)`
  is unique by construction in `CDCCapture`. If a real source emits
  duplicate LSNs (impossible in Postgres, possible only in misconfigured
  pipelines), the lake's PK on `sequence` catches it; the warehouse merge
  catches it as a same-sequence no-op.
- **Multi-tenant catalogs** — one domain only.
- **Lineage / column-level provenance** — out of scope; production would
  use OpenLineage or DataHub lineage.
- **Authorization on warehouse tables** — out of scope.

---

## What we did NOT compromise on

- **Idempotency** — every lake write is dedup'd by `sequence`, every
  warehouse merge is dedup'd by `_cdc_seq`. Replay is provably safe.
- **Out-of-order safety** — the `_cdc_seq` guard handles late-arriving
  smaller sequences. Tested in `test_older_event_does_not_clobber_newer_state`.
- **Stop-the-line** — `run_tick` aborts before any data movement on
  breaking drift. `CDCCapture` can also refuse writes when configured
  with a strict detector. Both modes are tested.
- **Validation parity** — the data-quality rules are *generated* from
  contracts, so every NOT NULL column / enum domain in the source
  appears as a downstream rule by construction.
- **Time travel** — SCD2 is real, not approximated. `state_at(ts)`
  goes through the same path the warehouse merge writes, so any new
  CDC event automatically becomes time-travelable.
- **Testing across categories** — 9 test modules, ~80 tests covering
  source constraints, CDC, lake, warehouse, schema, data quality,
  catalog, time-travel, and end-to-end.

---

## Tradeoffs we'd revisit with more time

1. **Replace the in-memory `CDCCapture` with a real Debezium stub** —
   the current shape is faithful but lacks the long tail of edge cases
   (transactional grouping, snapshot vs streaming mode).
2. **Add column-level lineage to the catalog** — the 13 datasets all
   know their primary keys but not where each column flows from.
3. **Add a freshness rule to data quality** — currently we check
   correctness, not staleness. Production needs both.
4. **Wire a tiny Streamlit UI on top of `state_at`** — would make
   time-travel demonstrable to non-engineers.
5. **Containerize** — a `Dockerfile` + `docker-compose.yml` with
   Postgres + Kafka + the orchestrator would be the natural next step
   to demonstrate the production analogue concretely.

None of these change the contracts; they're all add-ons to the same
core.
