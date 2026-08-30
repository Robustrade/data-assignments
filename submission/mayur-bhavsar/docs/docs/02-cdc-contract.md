# CDC contract

## What a change is

One row in `cdc_events`, written by an `AFTER INSERT / UPDATE / DELETE` trigger
inside the source transaction:

| Field | Meaning |
|---|---|
| `event_id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. Strictly increasing, never reused. The replay offset. |
| `table_name`, `op` | Which table, and `I` / `U` / `D`. |
| `pk` | Canonical JSON of the primary-key columns — composite for weak entities. |
| `before_image` | Full row before the change. `NULL` for an insert. |
| `after_image` | Full row after the change. `NULL` for a delete. |
| `committed_at` | Source clock, UTC, millisecond resolution. |

A `CHECK` on `cdc_events` enforces the shape: an insert has no before image, a
delete has no after image, an update has both. The change log validates itself.

Because capture is a trigger, it cannot be bypassed by an application that
forgets to emit an event, and it sees deletes — which a timestamp-watermark
extraction never can.

### The cascade trap

SQLite does **not** fire delete triggers for rows removed by `ON DELETE CASCADE`
unless `PRAGMA recursive_triggers` is on. Without it, deleting an order would
reach the lake while its line items vanished silently, leaving orphans in the
warehouse forever. The pragma is set in `cdclake/source.connect` and the
behaviour is pinned by `test_cascaded_child_deletes_are_captured`.

Every capture mechanism has an equivalent trap. Debezium's is the initial
snapshot boundary; a watermark extractor's is deletes and intra-second churn.

---

## Delivery semantics

> Transport is **at-least-once**. The lake write is idempotent under the
> `(table_name, event_id)` merge key. The checkpoint is derived from the
> manifest, which is only appended after the data files are durable. A crash
> between those two steps therefore produces a **duplicate**, never a **loss**,
> and the warehouse — being a pure function of the deduplicated lake — is
> unaffected. Net effect: **effectively-exactly-once**.

The trade is explicit: given a choice between possibly losing a change and
possibly seeing one twice, this design always chooses twice, and then makes the
duplicate harmless.

### Write protocol

```
1. gate         compare live schema to the published contract; halt if breaking
2. read         SELECT ... WHERE event_id > checkpoint ORDER BY event_id LIMIT n
3. write data   one file per table, written to *.tmp then atomically renamed
4. write manifest  one line, flushed and fsynced
```

Progress is only real after step 4. A failure at step 3 leaves files that no
reader will ever see, because **readers resolve files through the manifest**,
not by globbing the directory.

### Crash residue is self-healing

The batch id is `max(batch_id in manifest) + 1`. A crash never appends a
manifest line, so the retry allocates the *same* batch id and writes the *same*
file names — the atomic rename replaces the orphan rather than accumulating
beside it. `Lake.gc_orphans()` exists for the one case that is not
self-healing: a crash and its retry straddling midnight, so the retry lands in
a new `ingest_date` partition.

Both paths are covered by
[`tests/test_replay_and_recovery.py`](../tests/test_replay_and_recovery.py) and
demonstrated by `make crash-recovery`.

---

## Ordering

**Order is by `event_id`. Never by arrival, never by wall clock.**

The SCD2 models window by `partition by pk_hash order by _version`, where
`_version` is the source `event_id`. Consequences:

- Landing the same change records in reverse order produces a byte-identical
  warehouse (`test_out_of_order_arrival_produces_the_same_warehouse`).
- A late-arriving update cannot clobber a newer state, because "newer" is
  defined by version, not by when the file appeared.
- Two changes to the same key inside the same millisecond produce a zero-length
  validity interval. That is correct — the state existed for under a
  millisecond — and it is why point-in-time queries resolve by version rather
  than trusting `valid_to`. See [docs/04](04-time-travel.md).

### Why `event_id` is a safe offset here, and would not be on Postgres

SQLite serialises writers, so `AUTOINCREMENT` values are assigned in commit
order: if the consumer has seen `event_id = 100`, no event with a lower id can
still arrive.

On Postgres that reasoning fails. A sequence value is allocated when the
statement runs, but the row becomes visible when the transaction commits, so a
long transaction can commit `id = 100` *after* a short one commits `id = 101`.
A consumer scanning `id > 101` would skip it permanently. This is the classic
audit-table gap problem, and it is the real reason production CDC reads the WAL
LSN — which *is* assigned in commit order — rather than a sequence.

Stated plainly: the offset semantics here are correct for this engine, and
swapping the engine without swapping the offset would introduce silent data
loss. [ADR-0001](adr/0001-cdc-capture-mechanism.md) covers the migration.

---

## Checkpointing and replay

The checkpoint is not stored. It is computed:

```python
checkpoint = max(batch["checkpoint_after"] for batch in manifest) or 0
```

There is therefore no second source of truth that can drift out of sync with
the data. A restarted process reads the manifest and resumes exactly where the
last durable batch ended (`test_restart_resumes_from_the_checkpoint`).

| Scenario | Behaviour |
|---|---|
| Nothing new | `run_batch` returns `None`; no batch, no file, no manifest line |
| Restart mid-stream | Resume from the manifest; no re-read of committed events |
| Crash before manifest | Same events re-read; same batch id; files overwritten |
| Duplicate delivery | Absorbed by `(table_name, event_id)` in `stg_cdc_events` |
| Full replay from zero | Rebuild the warehouse from the lake; result is identical |
| Recovery from history | Delete the warehouse entirely and rebuild; result is identical |

---

## Deletes

A delete is a **tombstone**, never an erasure:

- The lake keeps the `op = 'D'` record with the full before image, forever.
- The SCD2 model closes the previous interval and emits a version with
  `is_deleted = true`, carrying the last known attribute values via
  `coalesce(after, before)`. Without that coalesce the tombstone would be an
  all-NULL row and the moment *before* the delete would be unrecoverable.
- `*_current` filters tombstones out.
- `dbt_tests/assert_deletes_are_tombstoned_not_erased.sql` proves every delete
  event in staging has a matching flagged history row.

---

## Near real time

"Near real time" is a number, not an adjective:

- **SLO:** end-to-end source-commit to lake-durable lag under **60 seconds**
  (`cdclake.quality.FRESHNESS_SLO_SECONDS`).
- **Measured:** every batch writes `lag_p50_seconds` and `lag_max_seconds` to
  `_state/metrics.ndjson`, alongside `backlog_after`.
- **Checked:** `check_freshness` fails the quality run if the worst observed lag
  exceeds the SLO. Observed in practice at this scale: single-digit
  milliseconds.

The batch interval is the tunable knob. Continuous invocation of `run_batch`
gives sub-second lag at the cost of more, smaller files; a scheduler running it
every 30 seconds sits comfortably inside the SLO with fewer.

---

## The seam

```python
class CdcSource(Protocol):
    def read(self, after_event_id: int, limit: int) -> list[RawChange]: ...
    def high_watermark(self) -> int: ...
```

`TriggerAuditSource` is the implemented backend. A WAL backend implements the
same two methods: `event_id` becomes the LSN, `high_watermark` becomes the
replication slot's confirmed flush position. Nothing downstream changes —
`IngestionRunner`, the lake, the models and every assertion are untouched.

That seam is the whole point of [ADR-0001](adr/0001-cdc-capture-mechanism.md).
