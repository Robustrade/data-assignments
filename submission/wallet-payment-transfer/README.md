# Wallet / Payments / Transfers — CDC Lakehouse

A minimal, end-to-end CDC pipeline that mirrors a wallet/payments/transfers
source system into a **lake** (immutable change history) and a **warehouse**
(current-state + SCD2 history). It detects schema drift, stops the line on
incompatible changes, and runs idempotently under replay and restart.

It is built on **Python + DuckDB** so the entire pipeline — source,
ingestion, lake, warehouse, schema detection, validation, and catalog —
runs in a single process with no external services.

---

## Quick start

```bash
cd submission/wallet-payment-transfer

# 1. install deps
pip install -r requirements.txt

# 2. run all tests (CDC, lake, warehouse, schema, time-travel, e2e, …)
pytest -q

# 3. run the end-to-end pipeline demo
python -m scripts.run_pipeline

# 4. run the time-travel demo (SCD2 reconstruction at multiple timestamps)
python -m scripts.demo_time_travel

# 5. run the schema-change / stop-the-line demo
python -m scripts.demo_schema_change
```

CI gates (see `github_pr_checks_data.md`):

```bash
python -m scripts.check_schema_contracts     # gate 4: schema-contract-check
python -m scripts.run_data_quality_checks    # gate 5: data-quality-check
python -m scripts.validate_catalog           # gate 6: catalog-check
pytest -q                                    # gate 2: test-pipeline
```

All five exit non-zero on failure.

---

## What's in the box

```
submission/wallet-payment-transfer/
├── source/                    # source layer
│   ├── models.py              #   DDL: 6 tables, indexes, CHECK constraints
│   ├── contracts.py           #   versioned schema contracts (frozen dataclasses)
│   └── seed.py                #   canonical seed (3 customers → 5 ledger entries)
│
├── pipeline/                  # CDC + lake + warehouse + schema detection
│   ├── cdc.py                 #   CDCRecord + CDCCapture (with strict-schema mode)
│   ├── lake.py                #   append-only change log (lake_cdc_events)
│   ├── warehouse.py           #   current-state + SCD2 + state_at + restore
│   ├── schema_detector.py     #   8 drift rules with BREAKING/WARN severities
│   ├── checkpoint.py          #   per-stage sequence checkpoint store
│   └── orchestrator.py        #   run_tick() — bootstrap, drift gate, lake → wh
│
├── catalog/
│   └── catalog.json           # 13 datasets (1 lake + 6 wh_* + 6 wh_*_history)
│
├── scripts/                   # CI entry points + demos
│   ├── check_schema_contracts.py
│   ├── run_data_quality_checks.py
│   ├── validate_catalog.py
│   ├── run_pipeline.py
│   ├── demo_time_travel.py
│   └── demo_schema_change.py
│
├── tests/                     # 9 test modules, ~80 tests across all categories
│   ├── test_source_schema.py
│   ├── test_cdc.py
│   ├── test_lake.py
│   ├── test_warehouse.py
│   ├── test_schema_contracts.py
│   ├── test_data_quality.py
│   ├── test_catalog.py
│   ├── test_time_travel.py
│   └── test_e2e.py
│
├── ARCHITECTURE.md            # detailed design, data flow, layer contracts
├── ASSUMPTIONS.md             # tradeoffs, simplifications, production analogues
├── PR_DESCRIPTION.md          # populated PR template
└── README.md                  # this file
```

---

## Domain (very short)

Customers register, link payment methods, hold wallets, and move money
through transfers. Every balance change is mirrored in an immutable
ledger. Strong entities own lifecycle: `customers`, `wallets`, `transfers`.
Weak entities depend on a parent: `payment_methods` (← customer),
`transfer_events` (← transfer), `ledger_entries` (← wallet).

Important invariants (enforced at source AND in the warehouse via
parity rules):

- `wallet.balance >= 0`, `transfer.amount > 0`
- `transfer.from_wallet_id != transfer.to_wallet_id`
- `transfer.settled_at >= transfer.created_at` (when settled)
- `SUM(ledger_entries.signed_amount) per wallet == wallet.balance`
- transfer status follows a documented state machine

See `ARCHITECTURE.md` for the full model.

---

## Reading order for reviewers

If you have ten minutes, read these in order:

1. `ARCHITECTURE.md` — single-page overview of the data flow
2. `pipeline/cdc.py` — CDC record contract + sequence semantics
3. `pipeline/warehouse.py` — idempotent merge + SCD2 + time travel
4. `pipeline/schema_detector.py` — the 8 drift rules
5. `tests/test_e2e.py` — the full lifecycle in one file
6. `PR_DESCRIPTION.md` — the deliverable narrative

The whole pipeline is ~1,400 LOC and the tests are ~1,300 LOC.

---

## Production analogue

This project is intentionally a single-process DuckDB simulation. The
production analogue is straightforward:

| In this project              | Production stand-in                                          |
|------------------------------|--------------------------------------------------------------|
| `CDCCapture` + `sequence`    | Postgres logical decoding / Debezium → Kafka topic           |
| `lake_cdc_events`            | Kafka + S3/GCS Iceberg/Delta bronze table partitioned by day |
| `wh_<table>` + `_<table>_history` | Snowflake / BigQuery / Redshift gold tables             |
| `pipeline_checkpoints`       | Kafka consumer group offset committed with the warehouse merge |
| `SchemaDetector`             | Schema registry + dbt `source.freshness` / Great Expectations |
| `catalog/catalog.json`       | Atlan / DataHub / Unity Catalog                              |
| `bootstrap()` + `run_tick()` | Airflow / Dagster sensor + task                              |

`ASSUMPTIONS.md` covers each tradeoff explicitly.
