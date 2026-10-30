# Test Plan — CDC Lakehouse (E-commerce Orders)

## 1. Approach

- **Real path, no shortcuts.** Tests write SQL to the SQLite source; triggers produce
  `change_log` events; the pipeline moves them to the lake and warehouse. Assertions are
  made at every hop and across hops.
- **Isolated and deterministic.** Each test gets a fresh `source.db` and `platform.duckdb`
  in pytest's `tmp_path`. No sleeps, no network, no shared state.
- **Red → Blue → Green.** Each requirement starts as a failing test (Red), gets the minimum
  code to pass (Blue), then is refactored with the test still green (Green).
- Test IDs below are referenced from the test function docstrings so every requirement is
  traceable to a test.

## 2. Test Harness

| Fixture / hook | Purpose |
|---|---|
| `platform` | Builds source + pipeline + DuckDB in `tmp_path`; exposes `source` write helpers and `pipeline` |
| `clock` | Fake clock injected everywhere instead of `datetime.now()`; tests advance time explicitly |
| `fault_after(n)` | Makes the pipeline raise after *n* events inside a batch — simulates a crash mid-commit |
| `inject_events(...)` | Feeds hand-made events to the pipeline, bypassing the source — for out-of-order, duplicate and invalid data |
| `snapshot_source()` | Reads all 9 source tables, normalised (paise → rupees, text → timestamps) for comparison with the warehouse |
| `seed_basic()` | Users, wallets with top-ups, products with inventory |

## 3. Scenarios

### 3.1 Source model and constraints — `tests/test_source_model.py`
| ID | Scenario | Expected |
|---|---|---|
| SRC-01 | Duplicate PK, unknown FK, NULL in NOT NULL column, value outside enum | Write rejected |
| SRC-02 | Negative amount / balance; `reserved > on_hand`; wrong `line_total` | Write rejected (CHECK) |
| SRC-03 | Illegal status transition (e.g. `delivered → pending`) | Rejected by trigger |
| SRC-04 | Update or delete a `wallet_transactions` row | Rejected by trigger |
| SRC-05 | Wallet payment without `wallet_txn_id`; card payment without `provider_ref` | Rejected (CHECK) |
| SRC-06 | Expected indexes exist | Present in `sqlite_master` |
| SRC-07 | Every insert/update/delete on all 9 tables | One `change_log` row with correct op, before/after |
| SRC-08 | Multi-table write helper (`place_order`) | All its events share one `txn_id` |
| SRC-09 | Update a primary key; delete a user or wallet | Rejected by trigger (S14) |
| SRC-10 | Order placed / cancelled / shipped; order exceeding available stock | `reserved` and `on_hand` move as in SCHEMA.md inventory flow; over-stock order rejected |
| SRC-11 | Delete a product that was never ordered | Product and inventory deleted in one transaction; two delete events, same `txn_id` |
| SRC-12 | Write that bypasses the helpers | Event gets a generated `txn_id` |
| SRC-13 | Every event | Stamped with the trigger's contract `schema_version` |

### 3.2 CDC correctness — `tests/test_cdc.py`
| ID | Scenario | Expected |
|---|---|---|
| CDC-01 | Insert, update, delete flow through | Lake has 3 events; warehouse current reflects final state; delete is a tombstone |
| CDC-02 | Same batch processed twice | Lake and warehouse unchanged on second run |
| CDC-03 | Crash after *n* events, then restart | Final state identical to an uninterrupted run; no gaps, no duplicates |
| CDC-04 | Older event arrives after a newer one | Newer state kept in current; both recorded in lake |
| CDC-05 | Batch size cuts a source transaction in half | Batch extended; whole transaction applied together |
| CDC-06 | Restart with existing checkpoint | Only events after the checkpoint are read |
| CDC-07 | Late update to a deleted row | Row stays deleted |
| CDC-08 | One cycle after a committed source change | Change visible in warehouse; `lag_events` = 0, freshness = 0 |
| CDC-09 | `bootstrap()` on a source that already holds data, with writes during the snapshot | Warehouse == source; overlapping events applied without duplicates |

### 3.3 Schema change safety — `tests/test_schema_contract.py`
| ID | Change made to the source | Expected |
|---|---|---|
| SCH-01 | Drop a column | `STOPPED`, alert, nothing written for the batch |
| SCH-02 | Rename a column | `STOPPED` (seen as drop + add) |
| SCH-03 | Change a column type | `STOPPED` |
| SCH-04 | Nullable ↔ NOT NULL | `STOPPED` |
| SCH-05 | Add or remove an enum value in a CHECK | `STOPPED` |
| SCH-06 | Event carries a value outside the contract enum | `STOPPED` (per-event check) |
| SCH-07 | Change primary key or foreign key | `STOPPED` |
| SCH-08 | Drop a table or a CDC trigger | `STOPPED` |
| SCH-09 | Add a nullable column / new table | Warning recorded; ingestion continues; extra column ignored |
| SCH-10 | While `STOPPED`, call `run_once()` | No-op; status unchanged |
| SCH-11 | Update contract, `resume()` | Ingestion continues from checkpoint; no events lost |
| SCH-12 | Backlog mixes events from contract N and N+1 | Each event validated against its own version; all loaded |
| SCH-13 | Event stamped with an unregistered `schema_version` | `STOPPED` |
| SCH-14 | Approved change: column added / dropped / retyped | Warehouse migrated; existing history rows unchanged; new versions use the new shape |

### 3.4 Warehouse correctness — `tests/test_warehouse.py`
| ID | Scenario | Expected |
|---|---|---|
| WH-01 | After a mixed workload | Warehouse views == source snapshot for all 9 tables |
| WH-02 | Rebuild warehouse from the full lake | == incrementally built warehouse |
| WH-03 | `as_of(t)` for several t | == source snapshot taken at t |
| WH-04 | History for one order | One version per change; `valid_to` of each = `valid_from` of the next; one current version |
| WH-05 | `rebuild_to(t)` | Current state == state at t; checkpoint == last seq ≤ t |
| WH-06 | Type conversion | Paise → `DECIMAL(18,2)`, text → `TIMESTAMP`, PII hashed |
| WH-07 | `as_of(t)` across tables mid-transaction boundary | Tables consistent with each other (whole transactions only) |
| WH-08 | Mistaken source update → `export_as_of` → write back through helpers | Source restored; lake holds mistake and fix; history shows both versions |
| WH-09 | `rebuild()` after deliberately corrupting warehouse rows | Warehouse == source again |

### 3.5 Validation parity — `tests/test_quality.py`
One failing case per source rule S1–S14 (see SCHEMA.md §6), injected past the source.
| ID | Rule | Expected |
|---|---|---|
| DQ-01 | S1 duplicate PK / orphan FK / NULL / bad enum | Batch blocked, status `FAILED` (orphan FK via a batch run; NULL, enum and duplicate PK via the `verify()` full scan, because the per-event contract check stops those events first) |
| DQ-02 | S2–S4, S6, S7 row-level rule broken | Batch blocked |
| DQ-03 | S8 ledger row updated | Batch blocked |
| DQ-04 | S5 illegal transition | Loaded; quarantined; alert |
| DQ-05 | S9 order total ≠ Σ lines | Quarantined; alert |
| DQ-06 | S10 Σ successful payments ≠ total | Quarantined; alert |
| DQ-07 | S11 refund exceeds line quantity or payment | Quarantined; alert |
| DQ-08 | S12 wallet balance ≠ ledger | Quarantined; alert |
| DQ-09 | Clean workload | All checks pass; results recorded |
| DQ-10 | S13 reserved ≠ open-order quantity | Quarantined; alert |
| DQ-11 | S14 key-changing update or user delete event | Batch blocked |
| DQ-12 | Rules registry vs SCHEMA.md S1–S14 | Every rule ID has a warehouse check (parity test) |
| DQ-13 | `FAILED`, source fixed, `resume(quarantine_seqs=…)` | Bad events in quarantine; fix events loaded; override recorded in `cdc.alerts` |
| DQ-14 | Contract-generated CHECKs vs warehouse checks | Same column rules on both sides |

### 3.6 Catalog — `tests/test_catalog.py`
| ID | Scenario | Expected |
|---|---|---|
| CAT-01 | Every lake and warehouse dataset | Has a catalog entry |
| CAT-02 | Each entry | Has layer, description, owner, consumers, cadence, PII, access, schema |
| CAT-03 | Catalog schema vs live table | Columns and types match |

### 3.7 Runtime — `tests/test_runtime.py`
| ID | Scenario | Expected |
|---|---|---|
| RUN-01 | `run_until_caught_up()` with a large backlog | Processes in several batches; lag 0 at end |
| RUN-02 | `run_forever()` with fake clock, idle source | Wait grows 0.2 s → 5 s |
| RUN-03 | `run_forever()` when a breaking change appears | Exits with `STOPPED` |

### 3.8 Operations — `tests/test_operations.py`
| ID | Scenario | Expected |
|---|---|---|
| OPS-01 | Second runner while a live lease exists | Refused with a clear error; stale lease can be taken over |
| OPS-02 | `init()` run twice | No error, no change; migrations applied once |
| OPS-03 | Freshness above warn / critical threshold (fake clock) | Warning / critical alert recorded |
| OPS-04 | `pause()` | `run_once()` processes nothing; `resume()` continues |
| OPS-05 | Delete `platform.duckdb`, re-run from checkpoint 0 | Lake and warehouse identical to before |
| OPS-06 | Run the demo workload twice from scratch | Identical lake, warehouse and output |
| OPS-07 | Config from `config.yml` overridden by env var | Override wins |

### 3.9 End to end — `tests/test_end_to_end.py`
| ID | Scenario | Expected |
|---|---|---|
| E2E-01 | Top up wallet → order 2 products → pay part wallet, part card (one failed UPI attempt first) → ship → deliver → partial refund to wallet and card | Every layer consistent: lake has every event; warehouse == source; `as_of` at each step matches; all quality checks pass |

## 4. Requirement Coverage

| Assignment requirement | Tests |
|---|---|
| Modeling and constraint tests | SRC-01…13 |
| CDC correctness (insert/update/delete, duplicates, restart) | CDC-01…09, RUN-01 |
| Schema change safety (detect, stop, signal) | SCH-01…14, RUN-03 |
| Warehouse correctness (snapshot, parity, history/restore) | WH-01…09, DQ-01…14 |
| Catalog | CAT-01…03 |
| Durability, reproducibility, operational safety | OPS-01…07 |
| Full flow | E2E-01 |

Extra tests beyond the matrix: CI scripts run as subprocesses with forced failures (`--drift`, `--inject-violation`, broken catalog), `python demo.py` run twice with identical stdout, and example catalog queries executed.

## 5. CI Mapping

| CI job | Runs |
|---|---|
| `test-pipeline` | `pytest submission/hari -q` |
| `schema-contract-check` | `scripts/check_schema_contracts.py` — builds the source and checks it against the contract; also runs a breaking change and asserts it is detected |
| `data-quality-check` | `scripts/run_data_quality_checks.py` — runs the E2E workload and all parity checks |
| `catalog-check` | `scripts/validate_catalog.py` — builds the platform and compares the catalog with live tables |
| `lint-python-or-sql` | `ruff check`, `black --check` |

## 6. Not Covered (and why)

- Real WAL / Kafka delivery semantics, concurrency and network faults — simulated via
  duplicates, reordering and crash injection instead.
- Performance and latency at scale — out of scope for a local assignment.
