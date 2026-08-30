# CDC Lakehouse — order-to-cash

A change-data-capture pipeline that keeps an append-only **lake** and a
current-state **warehouse** in step with a transactional source, stops itself
when the source schema changes incompatibly, and can reconstruct any prior
point in time.

Everything runs locally with no services: the source is SQLite (real DDL,
constraints, indexes and capture triggers), the lake is newline-delimited JSON
on disk, the warehouse is DuckDB modelled with dbt.

---

## Quick start

```bash
pip install -r requirements.txt

make demo             # seed -> workload -> ingest -> build -> reconcile
make stop-the-line    # break the source schema; watch ingestion halt and recover
make crash-recovery   # kill a batch mid-write; prove nothing is lost
make checks           # everything CI runs
```

`make demo` finishes with a source-versus-warehouse reconciliation; every row
should read `delta=0`.

There is nothing to clean up between runs — `make demo` starts from scratch and
all runtime artefacts live under `.work/`, which is gitignored.

---

## The shape of it

```
   source (SQLite)                lake (NDJSON)              warehouse (DuckDB + dbt)
   ┌────────────────┐   read      ┌───────────────┐  read    ┌────────────────────┐
   │ 6 tables       │ ─────────▶  │ cdc_events    │ ───────▶ │ stg_cdc_events     │
   │ AFTER triggers │  schema     │ append-only   │          │ dim_*_history SCD2 │
   │ → cdc_events   │  gate ⛔    │ + manifest    │          │ *_current          │
   └────────────────┘             └───────────────┘          │ mart_order_...     │
          │                              │                   └────────────────────┘
          │  contracts/*.json            │  checkpoint derived              │
          └──────────────────────────────┴── from the manifest ─────────────┘
                                                                    catalog/datasets.yml
```

| Layer | Where | What it guarantees |
|---|---|---|
| Source | `sql/`, `cdclake/source.py` | Keys, foreign keys, CHECK constraints, enum domains, indexes. Capture is a database trigger, so nothing can bypass it. |
| Contracts | `contracts/*.json`, `cdclake/contracts.py` | The published belief about each source table, and the compatibility policy. |
| Ingestion | `cdclake/ingest.py`, `cdclake/gate.py` | At-least-once read, idempotent write, checkpointing, fail-closed schema gate. |
| Lake | `cdclake/lake.py` | Every change, immutable, with a manifest that is the single source of truth for what is durable. |
| Warehouse | `models/`, `macros/`, `cdclake/warehouse.py` | SCD2 history, current-state views, an analytical mart, point-in-time restore. |
| Quality | `models/schema.yml`, `dbt_tests/`, `cdclake/quality.py` | 127 in-warehouse assertions plus 87 platform checks, including mechanical validation parity. |
| Catalog | `catalog/datasets.yml`, `cdclake/catalog.py` | 15 datasets published with owner, consumers, cadence, lineage and PII flags — validated against the live warehouse. |

---

## Read this first

Design docs come before code, and they carry the reasoning:

| Doc | Answers |
|---|---|
| [docs/00-architecture.md](docs/00-architecture.md) | How the layers fit together, and the four properties everything else rests on |
| [docs/01-source-model.md](docs/01-source-model.md) | Entities, strong vs weak, keys, indexes, invariants, change patterns |
| [docs/02-cdc-contract.md](docs/02-cdc-contract.md) | What a change is, delivery semantics, ordering, checkpoints, deletes, replay |
| [docs/03-schema-evolution.md](docs/03-schema-evolution.md) | The compatibility matrix and the stop-the-line state machine |
| [docs/04-time-travel.md](docs/04-time-travel.md) | SCD2 modelling, the two restore primitives, and their limits |
| [docs/05-validation-parity.md](docs/05-validation-parity.md) | The traceability matrix from source rule to warehouse assertion |
| [docs/06-catalog.md](docs/06-catalog.md) | What is published, for whom, and how drift is caught |
| [docs/07-ci-and-testing.md](docs/07-ci-and-testing.md) | How the seven CI jobs map onto this repository |
| [docs/runbook.md](docs/runbook.md) | Restore, replay, recover from a halt, backfill |
| [docs/adr/](docs/adr/) | The four decisions that shaped everything else |

---

## Commands

```bash
python -m cdclake demo                      # full run from a clean state
python -m cdclake ingest                    # drain the source into the lake
python -m cdclake build                     # rebuild + assert the warehouse
python -m cdclake status                    # checkpoint, backlog, halt state
python -m cdclake restore --entity customer_order --at-version 39
python -m cdclake restore --entity customer_order --as-of 2026-08-29T12:00:00.000Z
python -m cdclake acknowledge --operator you --note "why it is safe to resume"
python -m cdclake republish-contracts       # break glass: adopt the live schema
python -m cdclake fixture                   # refresh the committed lake extract
```

All of them accept `--workspace <dir>`; the default is `.work/`.

---

## Validation

| Command | What it proves | Result |
|---|---|---|
| `make test` | 89 pytest cases: constraints, capture, replay, crash, gate, snapshot, restore, parity, catalog | 89 passed |
| `make quality` | 87 platform checks + `dbt build` (14 models, 127 assertions) | 87/87, dbt PASS |
| `make contracts` | Contracts match the DDL; generated spec current; all 8 schema mutations classified correctly | PASS |
| `make catalog` | Metadata complete; catalog and models agree both ways; schemas match the live warehouse | PASS |
| `make lint` | `ruff check` + `black --check` | clean |
| `make stop-the-line` | Halt, persisted state, refused restart, break-glass resume | PASS |
| `make crash-recovery` | No loss, no duplicates, idempotent replay | PASS |

---

## Assumptions and simplifications

Stated in full in [docs/00-architecture.md](docs/00-architecture.md#simplifications-and-what-would-change-in-production).
The short version:

- **Capture is trigger-based, not WAL-based.** Same interface, different
  backend; [ADR-0001](docs/adr/0001-cdc-capture-mechanism.md) explains what
  changes with Debezium in front of Postgres.
- **The lake is NDJSON files, not Delta/Iceberg.** The manifest supplies the
  atomicity a real table format would; [ADR-0002](docs/adr/0002-local-stack.md)
  covers the trade.
- **Micro-batch, not streaming.** "Near real time" is a stated SLO of 60s
  end-to-end lag, measured every batch, not an adjective.
- **Single process.** Fine to roughly 5k changes/second; the merge key already
  makes horizontal fan-out safe.
