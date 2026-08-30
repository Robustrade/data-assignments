# ADR-0003: SCD2 history as the restore model, with a version-exact primitive

**Status:** accepted · **Date:** 2026-08-29 (amended after a test flake)

## Context

The warehouse must expose current state *and* support moving backwards in time
to restore data. Three ways to model that.

| Option | Restore story |
|---|---|
| **A. Current-state tables only** | None. Rejected immediately — it is the assignment's stated failure mode |
| **B. Periodic snapshots of the warehouse** | Restore to a snapshot boundary. Coarse, storage-hungry, and cannot answer "the state one change before the bad one" |
| **C. SCD2 history derived from the change log** | Restore to any point, because every version is a row |

## Decision

**C.** `dim_<entity>_history` holds one row per version with
`valid_from` / `valid_to` / `is_current` / `is_deleted`, derived purely from
`stg_cdc_events` and windowed by `_version` (the source `event_id`).
`<entity>_current` is a **view** over it — `where is_current and not is_deleted`
— so current state cannot disagree with history.

Point-in-time resolution ranks by version rather than testing a
`valid_from <= t < valid_to` range:

```sql
where valid_from <= ?           -- or: _version <= ?
qualify row_number() over (partition by pk_hash order by _version desc) = 1
```

## Consequences

**Good.** Any point in time is reachable. Deletes are recoverable, because a
tombstone carries the last known attribute values via `coalesce(after, before)`
rather than being an all-NULL row. Status *transitions* become rows, which is
what makes `assert_legal_order_status_transitions.sql` expressible at all — a
snapshot-only warehouse can validate a state but never the path taken to it.

**Cost.** History grows with change volume, and the window functions are the
expensive part of the build. Hence history models are materialised as tables
and current-state models as views: the expensive thing is computed once, the
cheap thing cannot drift.

**Cost.** Every entity needs a stable key. `pk_hash` — a hash of the canonical
primary-key JSON — makes composite keys on weak entities merge exactly like
surrogate keys on strong ones.

## Amendment: why there are two restore primitives

The first implementation exposed only `as_of(entity, timestamp)`, and
`test_a_deleted_entity_can_be_recovered_from_before_the_delete` was
intermittently red. The cause was not a bug in the model: the cancellation and
the delete sometimes committed inside the same millisecond, so "one millisecond
before the tombstone" landed before *both*.

The tempting fixes — widen the tolerance, slow the workload down — would both
have hidden a real property: **the source clock has millisecond resolution, so
no timestamp can separate two changes committed inside the same millisecond.**

So `as_of_version(entity, version)` was added. Event ids are a total order over
the change log, so it is never ambiguous, and it is the primitive a restore
should use. `as_of` remains, with defined semantics — the latest version at or
before the instant, deterministic under collision, asserted by
`test_timestamp_restore_resolves_collisions_to_the_latest_version` — because a
timestamp is what an operator actually has during an incident.

This is the same split Delta Lake draws between `TIMESTAMP AS OF` and
`VERSION AS OF`. The flake is the reason I now understand why they bothered.

There is exactly one implementation of each, in `cdclake/warehouse.py`. A dbt
macro version was written and then deliberately deleted: a second copy of the
rule would have been free to drift from the one operators actually run.
