# ADR-0002: SQLite + NDJSON + DuckDB instead of Postgres + Delta

**Status:** accepted · **Date:** 2026-08-29

## Context

The reviewer runs `pytest`, `dbt` and three Python scripts inside the
submission folder. There are no service containers and no network, and the
"README tested from a clean state" criterion means anything requiring
`docker compose up` is a step where a review can stall.

Three storage decisions had to be made: source engine, lake format, warehouse
engine.

## Decision

| Layer | Chosen | Instead of |
|---|---|---|
| Source | SQLite (stdlib `sqlite3`) | Postgres in Docker |
| Lake | Newline-delimited JSON + a manifest | Delta Lake or Iceberg |
| Warehouse | DuckDB, modelled with dbt-duckdb | Snowflake / BigQuery / Databricks SQL |

## Consequences

### Source: SQLite

**Good.** Real DDL with real primary keys, foreign keys, `CHECK` domains,
unique constraints, indexes and triggers — the modelling exercise is intact.
Zero setup. `PRAGMA foreign_key_check` and `PRAGMA integrity_check` give the
quality layer genuine source-side assertions.

**The one real cost is money.** SQLite has no exact decimal type; a
`DECIMAL(12,2)` column gets NUMERIC affinity and is physically stored as a
float. Mitigated on both sides: every monetary `CHECK` is written with
`ROUND(x, 2)` so values stay at two decimal places, and the warehouse casts
once to `DECIMAL(18,2)` on ingest, so all downstream arithmetic — including the
line-item-total assertion — is exact. `test_mart_totals_are_exact_decimals`
pins it. Postgres `NUMERIC(12,2)` would be exact at the source too, and nothing
downstream would change.

**Secondary cost.** No native `ENUM`, so domains are `CHECK ... IN (...)` and
the gate parses them out of `sqlite_master`. The published contract has the
same shape either way, so this is a parsing detail, not a modelling one.

**A trap it introduced.** SQLite does not fire delete triggers for rows removed
by `ON DELETE CASCADE` unless `PRAGMA recursive_triggers` is on. Found by a
test, not by reading; documented in [docs/02](../02-cdc-contract.md#the-cascade-trap).

### Lake: NDJSON + manifest

**Good.** The change log is readable text a reviewer can open, diff and reason
about. No extension downloads, so it works in a sandboxed CI. Most importantly
it forced the atomicity argument to be made explicitly — tmp-write, atomic
rename, fsynced manifest append, and the rule that readers resolve files
through the manifest — rather than delegating it to a table format and hoping.

**Bad.** No column pruning, no predicate pushdown, no compaction, no schema
evolution machinery, and JSON is verbose. Fine at 46 records; wrong at 46
million. The manifest is a hand-rolled transaction log, and a real one is
better tested than mine.

**Note.** Storing `pk` / `before` / `after` as JSON *text* rather than nested
structs is not a consequence of this choice — it is deliberate and would be
kept on Delta. It is what makes the physical lake schema independent of the
source schema, so drift can never corrupt the change log itself.

### Warehouse: DuckDB

**Good.** Exact `DECIMAL`, `QUALIFY`, window functions, `read_json` with a
pinned column list, and dbt-duckdb. In-process, so tests are fast.

**Bad.** Single node. The models are portable — only the profile changes — so
that migration is configuration, not a rewrite.

**One wrinkle worth recording.** dbt keeps a read-write handle to the DuckDB
file inside the same process, and DuckDB refuses a second connection whose
configuration differs. `Warehouse.connect` therefore does not open read-only;
read-only worked in isolation and failed the moment a build and a query shared
a process, which is exactly what the tests and the CI scripts do.

## The threshold

These choices are right for a reviewable, reproducible demonstration and wrong
for production. Stated concretely: **beyond roughly 5,000 changes per second,
or beyond a single-node working set**, move to Postgres + Debezium +
Delta/Iceberg + a distributed warehouse. The `(table_name, event_id)` merge key
already makes partitioned parallel consumers safe, so the ingestion side scales
by partitioning on `table_name + hash(pk_hash)` without a design change.
