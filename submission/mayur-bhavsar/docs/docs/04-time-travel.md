# Historical recovery and time travel

Three distinct things, deliberately kept separate:

| | Holds | Answers |
|---|---|---|
| **Lake** | Every change, forever, immutable | "What happened, and in what order?" |
| **History** (`dim_*_history`) | One row per version, with validity intervals | "What did this row look like at time T / version N?" |
| **Current** (`*_current`) | The open interval, tombstones excluded | "What is true now?" |

Current is a view over history; history is derived from the lake. Neither can
disagree with what is beneath it, because neither is materialised
independently.

---

## The SCD2 model

`macros/warehouse.sql:scd2_from_cdc` turns the change log into versions:

```sql
windowed as (
    select *,
        lead(committed_at) over w as _next_committed_at,
        lead(_version)     over w as _next_version
    from typed
    window w as (partition by pk_hash order by _version)
)
select
    pk_hash,
    pk_hash || ':' || cast(_version as varchar) as scd_key,
    <entity columns>,
    _op,
    _version,
    committed_at              as valid_from,
    _next_committed_at        as valid_to,
    _next_version is null     as is_current,
    _op = 'D'                 as is_deleted
from windowed
```

Four things worth noting:

- **`partition by pk_hash`** — a hash of the canonical primary-key JSON, so
  composite keys on weak entities merge exactly like surrogate keys on strong
  ones.
- **`order by _version`** — the source `event_id`. Not arrival, not wall clock.
  This is what makes the model a pure function of the lake.
- **`coalesce(after, before)`** when extracting attributes — a delete carries
  only a before image. Without this the tombstone would be an all-NULL row and
  the state immediately before the delete would be unrecoverable.
- **`is_deleted`, not row removal** — deletes close their interval and stay.

Structural invariants are asserted by
`dbt_tests/assert_one_current_version_per_key.sql` (exactly one open interval
per key, across all six entities) and
`dbt_tests/assert_scd2_intervals_are_ordered.sql` (no interval ends before it
starts; closed intervals are never current; open intervals always are).

---

## The two restore primitives

```python
warehouse.as_of(entity, "2026-08-29T12:00:00.000Z")   # by time
warehouse.as_of_version(entity, 39)                    # by change version
```

Both resolve the same way — take every version at or before the point, keep the
highest `_version` per key, drop tombstones:

```sql
select *
from dim_{entity}_history
where valid_from <= strptime(?, '%Y-%m-%dT%H:%M:%S.%fZ')   -- or: _version <= ?
qualify row_number() over (partition by pk_hash order by _version desc) = 1
```

**Why `qualify` on `_version` rather than a `valid_from <= t < valid_to`
predicate.** The source clock has millisecond resolution, so two changes to the
same row can share a `committed_at`, producing a zero-length interval that a
half-open range predicate would skip entirely — silently returning the wrong
version, or none. Ranking by version is correct in every case and needs no
special handling for the boundary.

**Why there are two primitives.** The consequence of millisecond resolution is
that *no timestamp can separate two changes committed inside the same
millisecond*. `as_of` gives a well-defined answer there — the later version
wins, deterministically, asserted by
`test_timestamp_restore_resolves_collisions_to_the_latest_version` — but if you
need to land *between* two specific versions, only the version can express it.
Event ids are a total order over the change log, so `as_of_version` is never
ambiguous.

This is the same distinction Delta Lake draws between `TIMESTAMP AS OF` and
`VERSION AS OF`, for the same reason. Timestamps are for humans; versions are
for restores.

There is exactly one implementation of each, in `cdclake/warehouse.py`
(`AS_OF_SQL`, `AS_OF_VERSION_SQL`). A second copy as a dbt macro was
deliberately removed: it would have been free to drift from the one operators
actually run.

---

## Restore, operationally

A restore is not a report. The rows `as_of_version` returns are what you write
back into the source — and the source still enforces every original constraint,
so a restore that produces invalid rows fails loudly rather than corrupting
anything. That round trip is asserted end to end by
`test_restore_output_can_be_written_back_to_the_source`, including the part
people forget: **the restore is itself a change**, so it flows back through CDC
and lands in the warehouse like any other insert.

```bash
# what broke, and when
python -m cdclake restore --entity customer_order --at-version 45

# the state one change earlier
python -m cdclake restore --entity customer_order --at-version 44

# by wall clock, for a human incident window
python -m cdclake restore --entity customer_order --as-of 2026-08-29T18:29:00.000Z
```

Full procedures — including restoring a parent and its weak entities together —
are in [runbook.md](runbook.md).

---

## What is recoverable, and from what

| Scenario | Recover from | Mechanism |
|---|---|---|
| A row was updated wrongly | History | `as_of_version(entity, bad_version - 1)` |
| A row was hard-deleted | History | Same. The tombstone carries the last known values |
| A parent and its children were cascade-deleted | History | One `as_of_version` per entity, at the same version |
| The warehouse was corrupted or dropped | Lake | Delete the database and rebuild; result is identical (`test_full_replay_from_zero_reproduces_the_warehouse`) |
| A model was wrong and shipped bad data | Lake | Fix the model and rebuild; no re-extraction from the source |
| The lake itself was lost | Nothing | The lake is the durable record. It is what must be backed up — and it carries PII, which the catalog flags |

That last row is the honest limit. Everything above the lake is reproducible;
the lake is not.

---

## Retention

Not implemented, and it is the first thing a production deployment needs.

The lake grows without bound, and it holds personal data (customer name, email,
phone in before/after images), so "keep everything forever" collides with
erasure obligations. The shape of the answer: retain full change history for a
bounded window, then compact older changes into a periodic snapshot plus the
tombstones — preserving the ability to restore to any *snapshot* boundary while
dropping intermediate versions. Erasure is handled by rewriting affected
records in place, which is the one operation this design otherwise forbids and
would need an explicit, audited exception.
