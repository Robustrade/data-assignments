# Warehouse and Time Travel

Covers: `pipeline/warehouse.py`.

## Model

The warehouse is the latest known state per row, built entirely by
replaying the lake — there is no separate write path into it. It is
always a function of "which lake events have been applied so far," which
is what keeps it consistent with the lake and makes rebuild/restore
possible.

Each warehouse table (`wh_customers`, `wh_orders`, `wh_order_items`,
`wh_payments`) mirrors its source table's columns/types, plus two extras:

- `_cdc_seq` — the sequence (Kafka offset) of the last lake event applied
  to this row.
- `_deleted` — soft-delete flag. A source delete never physically removes
  the warehouse row, since the row is still needed for history/restore.

Warehouse tables are typed loosely (no `NOT NULL`/`CHECK`/`FK`) so the
merge logic can stay generic across all four tables — this is why a
separate validation-parity layer exists (see
`schema_safety_and_data_quality.md`).

## Merge logic: last-write-wins

`apply_lake_events(conn, lake_rows)` does a full-column upsert per event:

```sql
INSERT INTO wh_x (...) VALUES (...)
ON CONFLICT (pk) DO UPDATE SET ...
WHERE excluded._cdc_seq > wh_x._cdc_seq
```

The `WHERE` clause on the upsert is the entire correctness guarantee: an
out-of-order or duplicate event can never clobber a row that's already
ahead of it, because the write is simply rejected if its sequence isn't
strictly newer than what's already there. Each event's `data` is the full
row snapshot at capture time, not a partial diff — applying it is always
a full-column overwrite, never a merge of individual fields.

This function is storage-agnostic by design: it takes a plain list of
event dicts, and doesn't care whether they came from a Parquet file, a
Kafka microbatch, or a test building them by hand. That's what lets
`pipeline/spark_consumer.py` reuse this exact function as its merge logic,
and lets the test suite exercise it without touching Kafka, Spark, or
Parquet at all.

## Time travel: `reconstruct_as_of`

```python
def reconstruct_as_of(lake_rows_up_to_bound: list[dict]) -> DuckDBPyConnection:
    snapshot_conn = duckdb.connect(":memory:")
    create_warehouse_tables(snapshot_conn)
    apply_lake_events(snapshot_conn, lake_rows_up_to_bound)
    return snapshot_conn
```

Given a list of lake events already bounded by the caller (e.g.
`sequence <= N`, or `captured_at <= some_timestamp`), this builds a
brand-new, throwaway, in-memory warehouse from scratch, using only those
events, and returns it. It never touches the live warehouse — this is
deliberately what "restore to a prior point in time" means operationally
here: rebuild the state somewhere else, verify it, then decide what (if
anything) to do with the live system.

The function itself doesn't know or care how the bound was chosen — that
responsibility belongs entirely to the caller, which keeps the function
simple and reusable for either a sequence-based or timestamp-based
restore.

### Verified live

For a single order (`tt_order1`) that moved `pending -> paid -> shipped`
across three real Kafka offsets (19, 20, 21):

| Query | Result |
|---|---|
| Live warehouse (current) | `shipped`, `_cdc_seq = 21` |
| `reconstruct_as_of(events <= 19)` | `pending` |
| `reconstruct_as_of(events <= 20)` | `paid` |

Three different, independently correct answers for three different
bounds, none of which touched the live warehouse.

## Note on the original design

The initial approach note (`DESIGN.md`) proposed Delta Lake as the
warehouse storage layer, using its native transaction-log-based version
history (`VERSION AS OF` / `TIMESTAMP AS OF`) for time travel. During
implementation this was simplified to plain DuckDB plus this explicit
`reconstruct_as_of` replay function instead. Trade-off: no ACID
transaction log or built-in version querying the way Delta gives you, but
no extra table-format dependency either, for a warehouse that's
single-node and assignment-scoped. The lake genuinely is just Parquet
(not Delta/Iceberg), so this isn't a "lakehouse" in the strict sense —
it's a data lake plus a replay-based restore mechanism.
