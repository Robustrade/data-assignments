# CDC Lakehouse Reliability Assignment — Submission

Domain: **Wallet / Payments / Transfers**. See `DESIGN.md` for the full
design writeup (required reading before the code).

## Stack

- **Source:** SQLite (simulating a transactional RDBMS; see `DESIGN.md` §2
  for what's simulated vs. what changes in production)
- **CDC:** Python, log-table based (WAL-equivalent) with checkpointed extraction
- **Lake:** append-only JSONL, date-partitioned
- **Warehouse:** DuckDB — latest-snapshot + SCD2 history tables
- **Validation:** custom Python assertions mirroring source constraints
- **Catalog:** generated YAML metadata file
- **Tests:** pytest, Red-Blue-Green discipline (see `GIT_HISTORY.md`)

## Setup

```bash
pip install -r requirements.txt
```

## Run the pipeline end-to-end

```bash
cd src
python3 source_db.py        # creates + seeds the source database
python3 cdc_extractor.py    # runs schema guard, then extracts new changes to the lake
python3 warehouse_loader.py # applies lake events to the warehouse (snapshot + history)
python3 catalog.py          # (re)generates catalog/catalog.yml
python3 validations.py      # runs validation parity checks, exits non-zero on failure
```

Or, from the repo root:

```bash
./run_pipeline.sh
```

## Run the tests

```bash
python3 -m pytest tests/ -v
```

17 tests across 4 files, covering every required category:
- `tests/test_modeling_and_catalog.py` — PK/FK/CHECK enforcement, catalog completeness
- `tests/test_cdc_correctness.py` — insert/update/delete capture, replay safety, restart-from-checkpoint
- `tests/test_schema_safety.py` — incompatible drift detection, stop-the-line, alert emission
- `tests/test_warehouse_correctness.py` — snapshot correctness, validation parity, time-travel reconstruction

## Simulating a schema break manually

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('data/source.db')
conn.execute('ALTER TABLE wallets DROP COLUMN status')
conn.commit()
"
cd src && python3 cdc_extractor.py   # should print 'CDC extraction HALTED' and exit 1
cat ../checkpoints/schema_alerts.log
```

## Repo layout

```
DESIGN.md                  design doc (read first)
01_source_schema.sql       production-target PostgreSQL DDL (enums, COMMENT ON, indexes)
src/
  source_db.py             schema + CDC-logging DML wrapper + seed data
  schema_guard.py           schema fingerprinting + drift detection
  cdc_extractor.py          checkpointed extraction, source -> lake
  warehouse_loader.py       lake -> warehouse (snapshot + SCD2 history), time travel
  validations.py            validation parity checks
  catalog.py                catalog metadata generator
tests/                      pytest suite (see above)
catalog/catalog.yml         generated dataset catalog
data/, checkpoints/, lake/  generated at runtime (git-ignored)
```

## Assumptions & Limitations

- Simulated CDC via an application-level change log rather than real
  Postgres WAL — documented in `DESIGN.md` §2, chosen for local
  reproducibility within the assignment's time-box.
- Schema-drift detection treats new tables/columns as backward-compatible
  and does not halt on them (documented simplification).
- Out-of-order arrival across independent producers isn't fully solved —
  ordering here relies on a single monotonic log; noted as a production gap.
- No production-scale orchestration (Airflow/Dagster) — a `run_pipeline.sh`
  script stands in; in production each step here maps 1:1 to a DAG task.

## Responsible AI usage

Claude (Anthropic) was used to draft the initial schema, CDC/warehouse
logic, tests, and this documentation from a written spec. All code was run
and verified locally (pipeline executed end-to-end, full test suite passed)
before submission, and the design tradeoffs above reflect decisions reviewed
and made deliberately rather than accepted as generated. See PR description
for the full disclosure.
