# CDC Lakehouse — E-commerce (Interview MVP)

Minimal CDC pipeline: **MySQL** source (trigger outbox) → **Parquet lake** (append-only events) → **DuckDB warehouse** (current-state snapshots) with schema contract gate, data quality checks, catalog, and point-in-time restore from the lake.

## Architecture

```
MySQL business tables
  └─ triggers → cdc_events (I/U/D)
        └─ Python ingest (checkpoint on event_id)
              ├─ schema fingerprint + contract check (stop-the-line)
              ├─ lake/source_table=*/dt=*/*.parquet
              └─ warehouse/warehouse.duckdb (wh_* tables)
```

**Production analogue:** Debezium/binlog → Kafka → object storage (Iceberg/Delta) → Snowflake/BigQuery; schema registry for contracts.

## Prerequisites

- Docker (for MySQL)
- Python 3.11+

## Quick start

```bash
cd submission/cdc-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d
# Wait for MySQL healthy

python -m pipeline.ingest
python scripts/run_data_quality_checks.py
python scripts/validate_catalog.py
pytest -q
```

## Restore (time travel)

Rebuild warehouse state from lake events up to a timestamp:

```bash
python scripts/restore_warehouse.py --as-of 2026-05-26T12:00:00+00:00
```

## Project layout

| Path | Purpose |
|------|---------|
| `source/schema.sql` | MySQL DDL (6 tables + `cdc_events`) |
| `source/triggers.sql` | CDC outbox triggers |
| `source/seed.sql` | Sample transactional data |
| `pipeline/ingest.py` | Ingestion entrypoint |
| `contracts/` | Schema contract + fingerprint baseline |
| `lake/` | Append-only Parquet CDC events (generated) |
| `warehouse/warehouse.duckdb` | Current-state analytics tables (generated) |
| `catalog/catalog.json` | Dataset metadata |
| `scripts/` | Quality, catalog, restore, contract checks |

## Schema change safety (source-owned contract)

The **source team** owns the schema. Data engineering does not bootstrap truth from live MySQL.

| Artifact | Owner | Purpose |
|----------|-------|---------|
| `contracts/schema_contract_spec.json` | Source team | Authoritative columns, types, nullability, keys |
| `contracts/schema_fingerprint.json` | Compiled from contract | Expected structural hash per table |

**Every ingestion** validates live `INFORMATION_SCHEMA` against the contract (columns, types, unexpected columns, fingerprint). On mismatch → **stop**, write `ingest/FAILED_SCHEMA_CHANGE.md`, exit non-zero.

When the source team publishes a new schema version, update `schema_contract_spec.json` and recompile:

```bash
python scripts/compile_schema_fingerprint.py
```

## Tests

```bash
pytest -q
```

Unit tests cover lake I/O, warehouse merge/idempotency, contracts, restore filtering, and catalog shape. MySQL integration is exercised manually via Docker + `pipeline.ingest`.

## Responsible AI

Document in your PR how AI was used and what you validated manually.
