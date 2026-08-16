# Approach — CDC Lakehouse Reliability Assignment

This is an initial outline PR to confirm my understanding of the problem before I build the
full solution. It covers the domain I'll model, the planned architecture, and the key design
decisions I intend to make. Full implementation will follow in subsequent commits on this
same branch/PR.

## 1. Understanding of the Problem

The core ask is not "build an ETL pipeline" — it's "design a small but *safe* data platform"
that:

- Captures every change (insert/update/delete) from a transactional source.
- Writes that change history durably to a **lake** (append-only, full history, replayable).
- Maintains a **warehouse** with the latest snapshot, updated near real-time.
- Detects **incompatible** source schema changes and stops ingestion safely rather than
  silently corrupting downstream data.
- Supports **time travel / restore** — reconstructing a prior state from the warehouse.
- Mirrors source-side **validations** (system + business rules) in the warehouse.
- Publishes both lake and warehouse datasets to a **catalog** so they're discoverable.

Correctness, explicit reasoning about failure modes, and clean layering matter more than
breadth or polish. I'm scoping this to a single well-reasoned domain rather than trying to
cover many edge cases shallowly.

## 2. Chosen Domain

**Wallet / Payments / Transfers** — realistic transactional shape, natural strong/weak entity
split, and gives me meaningful business validations to mirror (non-negative balances, valid
status transitions, settlement-after-creation ordering).

Planned source schema (5 tables):

| Table | Type | Notes |
|---|---|---|
| `customer` | Strong entity | id, name, email, created_at |
| `wallet` | Strong entity | id, customer_id (FK), currency, status (enum), created_at |
| `transfer` | Strong entity | id, source_wallet_id (FK), dest_wallet_id (FK), amount (decimal), status (enum), created_at, settled_at (nullable) |
| `wallet_balance_history` | Weak entity | id, wallet_id (FK), balance_after (decimal), recorded_at — depends on `wallet` for existence |
| `payment_attempt` | Weak entity | id, transfer_id (FK), attempt_no, status (enum), failure_reason (nullable), created_at |

Indexes on FK columns and on `(wallet_id, recorded_at)` / `(transfer_id, attempt_no)` for
change-capture and lookup performance. Enum-like fields: `wallet.status`,
`transfer.status`, `payment_attempt.status`. Nullable fields: `transfer.settled_at`,
`payment_attempt.failure_reason`.

## 3. Planned Tech Stack

Kept intentionally light so the correctness story stays easy to follow:

- **Source:** PostgreSQL (real constraints, real WAL available if I go the log-based route).
- **CDC:** Simulated/polling-based CDC using a `updated_at`/monotonic version column plus a
  soft-delete flag, rather than full WAL/Debezium — documented as a simplification. If time
  allows, I'll note what changes for a WAL-based (Debezium/logical replication) approach.
- **Lake:** Append-only Parquet (or JSON) change files, one record per captured change, with
  operation type (`insert`/`update`/`delete`), before/after state, and a monotonic sequence
  number for replay.
- **Warehouse:** DuckDB (or Postgres) holding both a current-state snapshot and an SCD2-style
  history table per entity for time travel.
- **Validation:** Python/SQL assertions run as tests (pytest) against both source and
  warehouse.
- **Catalog:** A minimal YAML/JSON registry file listing each lake/warehouse dataset, schema,
  owner, and update cadence — with a note on what a production catalog (e.g. DataHub, Glue,
  Unity Catalog) would add.

## 4. CDC Strategy (planned)

- Each source table gets a `updated_at` timestamp and `is_deleted` flag; a checkpoint table
  tracks the last processed `(table, updated_at, id)` cursor per table.
- Each ingestion run pulls all rows with `updated_at > checkpoint`, writes them to the lake as
  change records, then advances the checkpoint only after a successful lake write.
- **Duplicates:** lake records are keyed by `(table, pk, updated_at)`; replays are idempotent
  because writes are upserts keyed on that tuple.
- **Deletes:** modeled as soft-deletes in source, captured as `op=delete` change records.
- **Restart/replay:** since the checkpoint only advances after a durable lake write, a crash
  mid-run just re-reads the same window — no data loss, no duplication downstream.

## 5. Schema Change Safety (planned)

- Before each ingestion run, compare the live source schema (column names/types/nullability)
  against a stored "expected schema" snapshot.
- Safe changes (new nullable column) are logged and ingestion continues.
- Breaking changes (dropped/renamed column, type change, tightened nullability, enum domain
  shrink) cause ingestion to **halt** before writing anything for that run, with a clear
  error/warning emitted (log line + non-zero exit / flagged status row).

## 6. Time Travel / Restore (planned)

- Lake is the source of truth for full history — any point-in-time state can be rebuilt by
  replaying change records up to a timestamp.
- Warehouse keeps an SCD2 table per entity (`valid_from`, `valid_to`, `is_current`) so recent
  history doesn't require a full lake replay; older restores fall back to lake replay into a
  scratch table.

## 7. Open Questions / Assumptions Going In

- Simulated (polling-based) CDC is acceptable per the assignment text — I'll call out the
  WAL-based production alternative explicitly rather than implement it.
- I'll implement SCD2 for at least `wallet` and `transfer` (the tables restore realistically
  needs to touch); I may keep `payment_attempt` append-only-only if time is short, and will
  note that tradeoff rather than hide it.
- AI usage: I'm using an AI assistant to help scaffold boilerplate (schema DDL, test
  skeletons) — all logic, correctness reasoning, and final validation will be reviewed and
  tested by me before submission, and I'll disclose specifics in the final PR description.

## 8. Next Steps

1. Source DDL + seed data.
2. CDC extraction script + checkpointing.
3. Lake writer (append-only change files).
4. Warehouse loader (snapshot + SCD2).
5. Schema-change detector.
6. Validation/test suite (Red → Blue → Green per requirement).
7. Catalog metadata file.
8. Final PR write-up covering all 7 required description points.