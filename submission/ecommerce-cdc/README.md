# E-commerce CDC Lakehouse

A change-data-capture pipeline that keeps a **lake** (full change history) and a
**warehouse** (latest snapshot + time travel) in sync with an e-commerce source
system, with schema-drift safety and validation parity.

> Design rationale and the full step-by-step approach live in
> [SOLUTION_APPROACH.md](./SOLUTION_APPROACH.md).

## Stack

- **PostgreSQL** — canonical source design (`source/postgres_ddl.sql`)
- **Python** — CDC capture, checkpointing, schema safety, orchestration
- **DuckDB** — lake + warehouse (object-storage / cloud-warehouse analogue)
- **pytest** — system + business validation

CDC runs in two interchangeable modes:

- **Simulated in-process** (default) — runs with no external services; used by the test
  suite and CI.
- **Real PostgreSQL source via Docker** (optional) — insert/update/delete in a real
  Postgres and capture the changes into the lake + warehouse. See
  [TESTING_POSTGRES.md](./TESTING_POSTGRES.md).

The production analogue (Debezium / Postgres WAL) is documented at every layer.

## Layout

```
source/      Postgres DDL + DuckDB models + SCHEMA_CONTRACT
ingestion/   cdc capture · checkpoint · schema-contract safe-stop · pipeline · seed
lake/        append-only, idempotent CDC event history
warehouse/   current snapshot + SCD2 history + restore / time travel
validation/  system + business parity checks
catalog/     catalog.json dataset registry
postgres/    optional real-Postgres source bridge (snapshot-diff CDC via psycopg)
scripts/     CI gates (schema · data quality · catalog) + Postgres demo CLIs
tests/       red/blue/green tests across all categories
docker-compose.yml   local Postgres that auto-loads the source schema
```

## Setup & run (simulated, no external services)

```bash
cd submission/ecommerce-cdc
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

```bash
pytest -q                                  # full test suite (41 tests)
python -m ingestion.pipeline               # end-to-end demo (ingest + time travel + restore)

python scripts/check_schema_contracts.py   # schema safety gate
python scripts/run_data_quality_checks.py  # validation parity gate
python scripts/validate_catalog.py         # catalog gate
```

## Run against a real PostgreSQL source (optional, Docker)

Requires Docker (e.g. Colima); the Postgres driver (`psycopg`) is already in
`requirements.txt`. Full walkthrough: [TESTING_POSTGRES.md](./TESTING_POSTGRES.md).

```bash
docker-compose up -d                        # start Postgres (schema auto-created)

python scripts/pg_seed.py                   # insert realistic sample rows into Postgres
python scripts/pg_sync.py                   # capture changes -> lake + warehouse + report
python scripts/pg_mutate.py                 # apply updates/deletes (legal transitions)
python scripts/pg_sync.py                   # re-sync to see update/delete capture
```

Postgres connection: `localhost:5432`, db `kulu_ecom`, user/pass `kulu`/`kulu`.
The persistent lakehouse lives at `.data/lakehouse.duckdb`.

## What it demonstrates

| Requirement | Where |
|---|---|
| Source model (strong/weak entities, keys, indexes, enums) | `source/` |
| CDC capture + checkpoint/replay/restart | `ingestion/cdc.py`, `ingestion/checkpoint.py` |
| Lake retains every change (idempotent) | `lake/lake.py` |
| Warehouse latest snapshot + SCD2 time travel + restore | `warehouse/warehouse.py` |
| Schema-change detection + fail-closed stop | `ingestion/schema_contract.py` |
| Validation parity (system + business) | `validation/checks.py` |
| Catalog exposure | `catalog/catalog.json` |
| Reliability (duplicates, deletes, out-of-order, restart) | see SOLUTION_APPROACH §10 |
| Real Postgres source + CDC capture (optional) | `postgres/`, `scripts/pg_*.py`, `docker-compose.yml` |
