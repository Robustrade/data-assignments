# ADR-0001: Capture changes with source triggers, not a WAL reader

**Status:** accepted · **Date:** 2026-08-29

## Context

The pipeline must capture every insert, update and delete, survive restarts,
and support replay from a durable offset. Three mechanisms were available.

| Option | Strength | Cost |
|---|---|---|
| **A. Logical replication / WAL** (Postgres `wal_level=logical`, Debezium or a `psycopg2` slot consumer) | The real thing. No write amplification on the OLTP path; the LSN is assigned in commit order; sees columns nobody anticipated | Needs a running Postgres with replication configured. Cannot run in the assignment's CI, which has no service containers |
| **B. Trigger-based audit table** | Database-level, so an application cannot bypass it. Captures deletes and full before/after images. Deterministic ordering, which makes tests deterministic | Write amplification on every DML. Cannot see a column the triggers were not written for |
| **C. Timestamp watermark** (`WHERE updated_at > last_seen`) | Trivial | Cannot capture deletes at all. Misses intra-second churn. Breaks under clock skew |

The repository's CI runs `pytest` and `dbt` inside the submission folder with
no services available, so option A cannot be exercised by any check a reviewer
would actually run.

## Decision

Implement **B**, behind an explicit `CdcSource` protocol, and document **A** as
the production target. Reject **C** outright — a mechanism that cannot see
deletes fails the assignment's core requirement.

```python
class CdcSource(Protocol):
    def read(self, after_event_id: int, limit: int) -> list[RawChange]: ...
    def high_watermark(self) -> int: ...
```

## Consequences

**Good.** Capture is genuinely at the database level: `AFTER INSERT/UPDATE/DELETE`
triggers write full before and after images into `cdc_events` inside the source
transaction, so a change and its capture commit or roll back together. Deletes
and cascades are captured. Ordering is deterministic, so tests are deterministic
and the CI checks exercise real behaviour rather than a mock.

**The migration is a single class.** A WAL backend implements the same two
methods: `event_id` becomes the LSN, `high_watermark` becomes the slot's
confirmed flush position. `IngestionRunner`, the lake, every model and every
assertion are untouched.

**Two costs, both real.**

*Write amplification.* Every DML statement writes a second row. Acceptable at
this scale, not acceptable at high write volume — which is the threshold that
should trigger the move to A.

*Blindness to new columns.* The triggers enumerate columns explicitly, so a
column added to the source does not reach the lake until they are regenerated.
The gate raises a WARN saying exactly that. A WAL reader has no such blind spot.

**One subtlety that must not be carried across.** `event_id` is a safe replay
offset *here* because SQLite serialises writers, so `AUTOINCREMENT` values are
assigned in commit order. On Postgres a sequence value is allocated when the
statement runs but becomes visible at commit, so a long transaction can commit
id 100 after a short one commits 101 — and a consumer scanning `id > 101` would
skip it forever. Swapping the engine without swapping the offset to the LSN
would introduce silent data loss. It is written down in
[docs/02](../02-cdc-contract.md#why-event_id-is-a-safe-offset-here-and-would-not-be-on-postgres)
because it is exactly the kind of assumption that survives a migration
unnoticed.

## Alternatives reconsidered

Adding a `services: postgres` block to the CI workflow would have made A
possible. Rejected: the workflow belongs to the hiring repository and is a
required status check for every candidate. Editing another team's CI to suit
one's own submission is the wrong instinct; building to the constraint and
documenting the target is the right one.
