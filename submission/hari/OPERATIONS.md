# Operations — Running, Recovering and Restoring the Pipeline

How to run the pipeline reproducibly, what each status means, and what an operator does
when something goes wrong. Design background is in [README.md](./README.md).

---

## 1. Reproducibility

| Concern | How it is handled |
|---|---|
| Dependencies | `requirements.txt` pins exact versions (`duckdb==…`, `pyyaml==…`, `pytest==…`); Python 3.11+, SQLite from the standard library |
| Deterministic data | Demo and CI scripts use fixed IDs, a fixed seed and the fake clock, so two runs produce identical lake, warehouse and output |
| Idempotent setup | `init()` creates schemas/tables with `IF NOT EXISTS` and applies numbered migrations recorded in `cdc.migrations`; running it twice is a no-op |
| Configuration | `config.yml`, overridable by environment variables — no hard-coded paths |
| One command | `python demo.py` builds everything from nothing in `./data/` (git-ignored) |

`config.yml`

| Key | Env var | Default |
|---|---|---|
| `source_db` | `CDC_SOURCE_DB` | `data/source.db` |
| `contract_path` | `CDC_CONTRACT_PATH` | `contracts/source_contract.yml` |
| `platform_db` | `CDC_PLATFORM_DB` | `data/platform.duckdb` |
| `batch_size` | `CDC_BATCH_SIZE` | `500` events |
| `poll_min_s` / `poll_max_s` | `CDC_POLL_MIN_S` / `CDC_POLL_MAX_S` | `0.2` / `5` |
| `freshness_warn_s` / `freshness_crit_s` | `CDC_FRESHNESS_WARN_S` / `CDC_FRESHNESS_CRIT_S` | `60` / `300` |

---

## 2. Single Instance

Only one pipeline may write at a time; two writers could interleave checkpoints.

- DuckDB already allows only one read-write process per database file — a second process fails to open it.
- In addition, `run_forever()` takes a lease row in `cdc.pipeline_lock` (owner, heartbeat).
  A second runner sees a live lease and exits with a clear error; a lease whose heartbeat
  is older than 3 × `poll_max_s` is considered dead and can be taken over.

---

## 3. Pipeline Status

`cdc.pipeline_status` holds one row: status, reason, since, last batch, lag.
`run_once()` only processes events when the status is `RUNNING`. When it stops or fails the pipeline it records the status and alert first, then raises `SchemaContractViolation` or `QualityBlocked` so callers and scripts see the failure. Quarantine overrides given to `resume()` are stored in `cdc.quarantine_overrides`; `rebuild()` requires `PAUSED`. Snapshot (bootstrap) events use negative sequence numbers.

| Status | Set by | Meaning | Data written? |
|---|---|---|---|
| `RUNNING` | `init()`, `resume()` | Normal operation | yes |
| `PAUSED` | operator (`pause()`) | Deliberate hold, e.g. during a restore | no |
| `STOPPED` | schema contract check | Source no longer matches the contract | no |
| `FAILED` | blocking quality check or unexpected error | The batch could not be trusted; it was rolled back | no |

Every transition is also written to `cdc.alerts` (time, level, status, reason, details)
and logged. Scripts exit non-zero when the status is not `RUNNING`.

---

## 4. Runbook

### 4.1 `STOPPED` — incompatible schema change
1. Read the diff in `cdc.alerts` (table, column, expected vs actual).
2. Agree the change with the source team. Nothing is lost meanwhile — events keep
   accumulating in `change_log`.
3. Write contract version N+1 and the matching warehouse migration (see README §7).
4. `resume()` — events stamped with version N are validated against N, those stamped N+1
   against N+1; ingestion continues from the checkpoint.

### 4.2 `FAILED` — blocking quality check
The checkpoint did not move; the offending events are listed in `cdc.alerts`.
1. **Pipeline bug** (the source is right, our code is wrong): fix and deploy, then `resume()`.
   If already-loaded data was affected, rebuild the warehouse (§6.3).
2. **Bad data in the source** (e.g. a write that bypassed the source's rules):
   fix the data in the source — that creates new, correct events — then
   `resume(quarantine_seqs=[…])` to load the named bad events into `quality.quarantine`
   instead of blocking on them. The override is recorded in `cdc.alerts` with who and why.

### 4.3 Falling behind — freshness alert
`freshness = now − changed_at of the oldest unprocessed event` (0 when caught up) is
recorded every batch. Above `freshness_warn_s` → warning alert; above `freshness_crit_s` →
critical alert. Check the status (a non-`RUNNING` pipeline does not drain), then the batch
duration; `run_until_caught_up()` drains a backlog.

---

## 5. Recovery Matrix

| Lost or damaged | Recovery | Result |
|---|---|---|
| Warehouse tables | `rebuild()` — truncate warehouse, replay the whole lake | Identical to before (tested) |
| Lake **and** warehouse (`platform.duckdb` gone) | `init()`, checkpoint 0, `run_until_caught_up()` re-reads `change_log` from the start | Identical to before (tested) — possible because `change_log` is never pruned here |
| `change_log` pruned or source rebuilt | Lake is the only complete history; warehouse is unaffected | Capture restarts with an initial snapshot (README §4) |
| Crash during a batch | Nothing to do — the batch rolled back; next run re-reads it | No loss, no duplicates (tested) |

The checkpoint lives in the same DuckDB file as the lake and warehouse. That is what
lets them commit atomically, and it means they can never disagree with each other.

---

## 6. Restore Procedures

Three different needs, three different tools. The pipeline is `PAUSED` for 6.2 and 6.3.

### 6.1 Look at the past — `as_of(ts)`
Read-only; no pause needed. Returns every table as it was at source commit time `ts`
(e.g. "what did order O-17 and its payments look like at 10:05?").

### 6.2 Undo a bad change in the source — `export_as_of(table, keys, ts)`
For a mistaken update or delete in the application data.
1. `pause()`.
2. `export_as_of("orders", ["O-17"], ts)` produces the rows as they were at `ts`, plus the
   write-helper calls (or SQL) needed to put them back.
3. Apply them **to the source** through the normal write helpers. The restore is itself an
   ordinary change: it flows through CDC, is recorded in the lake, and appears as a new
   version in history — the audit trail shows both the mistake and the fix.
4. `resume()`.

### 6.3 Repair a corrupted warehouse — `rebuild()` / `rebuild_to(seq | ts)`
For a pipeline bug that wrote wrong data into the warehouse (the source and lake are fine).
1. `pause()` and deploy the fix.
2. `rebuild()` replays the whole lake with the fixed code.
   `rebuild_to(t)` stops at `t` instead — useful to inspect the warehouse exactly as it was
   built at that point; the status stays `PAUSED`, and `resume()` then replays forward.
3. `resume()`.

`rebuild_to` does **not** undo source changes: on resume the same events are applied
again. Undoing data is 6.2's job.

---

## 7. Personal Data

Email and phone are hashed in the warehouse; raw values exist only in the lake, which is
restricted. The lake is immutable, which conflicts with "delete my data" requests.
Out of scope here; the production answer is crypto-shredding — encrypt PII in the lake
with a per-user key and delete the key on request.
