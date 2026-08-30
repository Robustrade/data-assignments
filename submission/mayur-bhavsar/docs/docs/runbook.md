# Runbook

Day-two procedures. Every command here is real and exercised by a test or a
`make` target.

Set `WORKSPACE` (default `.work/demo`) or pass `--workspace` explicitly.

---

## 1. Is anything wrong?

```bash
python -m cdclake --workspace $WORKSPACE status
```

```
state       : RUNNING
checkpoint  : 46
backlog     : 0 event(s)
batches     : 7
orphan files: 0
```

| Field | Investigate when |
|---|---|
| `state` | Anything other than `RUNNING` — go to §4 |
| `backlog` | Growing across consecutive checks: the consumer is not keeping up |
| `orphan files` | Non-zero: a crash straddled a partition boundary. Harmless, see §5 |

Freshness and per-batch detail: `_state/metrics.ndjson`.
Alerts: `_state/alerts.ndjson`.

---

## 2. Point-in-time restore

**By version — exact, and what you want for a real restore.**

```bash
# find the change that did the damage
python -m cdclake --workspace $WORKSPACE restore \
  --entity customer_order --at-version 45 > after.json

# the state one change earlier
python -m cdclake --workspace $WORKSPACE restore \
  --entity customer_order --at-version 44 > before.json
```

**By timestamp — for a human incident window.**

```bash
python -m cdclake --workspace $WORKSPACE restore \
  --entity customer_order --as-of 2026-08-29T18:29:00.000Z
```

Note the limit: two changes to the same row inside the same millisecond cannot
be separated by a timestamp. `as_of` returns the later one, deterministically.
Use `--at-version` when that matters. ([docs/04](04-time-travel.md))

**The raw SQL**, if you would rather go straight at DuckDB:

```sql
select *
from dim_customer_order_history
where _version <= 44
qualify row_number() over (partition by pk_hash order by _version desc) = 1;
```

**Restoring a parent and its weak entities together.** Use the same version
boundary for each entity, so the result is internally consistent:

```bash
V=45
for E in customer_order order_item payment payment_attempt; do
  python -m cdclake --workspace $WORKSPACE restore --entity $E --at-version $((V-1)) \
    > restore_$E.json
done
```

**Writing it back.** The rows are shaped for the source, and the source still
enforces every constraint — an invalid restore fails loudly rather than
corrupting anything. The restore is itself a change: it flows back through CDC
and lands in the warehouse on the next cycle like any other insert.

---

## 3. Replay and rebuild

**Rebuild the warehouse from the lake** — after a bad model ships, or after the
warehouse is lost. No re-extraction from the source; the lake is sufficient.

```bash
rm $WORKSPACE/warehouse/warehouse.duckdb
python -m cdclake --workspace $WORKSPACE build
```

**Replay ingestion from an earlier offset.** The lake is append-only and the
merge key is `(table_name, event_id)`, so re-reading is safe: re-landed events
are absorbed. To force a re-read from offset N, truncate the manifest to the
batches at or below N — the checkpoint is derived from it:

```bash
head -n <batches-to-keep> $WORKSPACE/lake/_manifest/batches.ndjson > /tmp/m \
  && mv /tmp/m $WORKSPACE/lake/_manifest/batches.ndjson
python -m cdclake --workspace $WORKSPACE ingest
python -m cdclake --workspace $WORKSPACE build
```

Verify with `make quality` — reconciliation and lake completeness will both
fail loudly if the replay lost anything.

---

## 4. Recover from a HALTED pipeline

```bash
python -m cdclake --workspace $WORKSPACE status
```

```
state       : HALTED
reason      : 1 incompatible source schema change(s) detected
detected at : 2026-08-29T18:29:19.953Z
  - [BREAKING] COLUMN_DROPPED customer.phone
```

Full detail, including the classified diff: `_state/pipeline_state.json`.

Then decide, with the source team:

**(a) The change was a mistake.** Have it reverted, then acknowledge:

```bash
python -m cdclake --workspace $WORKSPACE acknowledge \
  --operator you --note "source reverted the accidental drop of customer.phone"
```

**(b) The change is intended.** Adopt it deliberately:

```bash
python -m cdclake --workspace $WORKSPACE republish-contracts   # bumps versions
python scripts/generate_dbt_entities.py                        # propagate
$EDITOR models/schema.yml                                      # update assertions
make contracts && make quality                                 # prove it holds
python -m cdclake --workspace $WORKSPACE acknowledge \
  --operator you --note "source team retired customer.phone in release 4.2"
```

`acknowledge` records who cleared the halt and why, in the state file and the
alert stream. It is deliberately separate from `republish-contracts`: accepting
a breaking change is a decision about the meaning of the data.

Walk the whole incident without touching anything real:

```bash
make stop-the-line MUTATION=drop_column
```

---

## 5. Orphan files after a crash

Normally none: a crashed batch is retried with the same batch id and overwrites
its own partial files. Orphans only survive if the crash and the retry straddle
midnight, so the retry writes into a new `ingest_date` partition.

They are invisible to readers — everything resolves files through the manifest
— so they cost disk, not correctness. To collect them:

```python
from cdclake.pipeline import build_platform
platform = build_platform("$WORKSPACE")
print(platform.lake.gc_orphans())
```

---

## 6. Backfill a new column

A compatible addition. The gate raises a WARN rather than halting.

1. The source team adds the nullable column.
2. **Regenerate the capture triggers** so the new column reaches the lake — the
   known limitation of trigger-based capture ([docs/03](03-schema-evolution.md#known-limitation)).
3. `python -m cdclake republish-contracts && python scripts/generate_dbt_entities.py`
4. Add any assertions the new column deserves to `models/schema.yml`.
5. `python -m cdclake build`.

Historical rows keep `NULL` for the new column, because the source genuinely
did not have that value at the time. Do not backfill it into history: that
would make the warehouse claim something the source never said.

---

## 7. Verify the platform end to end

```bash
make checks
```

Runs lint, the schema contract check, the data-quality run (87 platform checks
plus 127 in-warehouse assertions), catalog validation and the 89-test pytest
suite. Roughly two minutes from cold.
