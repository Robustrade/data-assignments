# CDC Lakehouse — E-commerce Orders

## Summary

This is a CDC pipeline that keeps a Parquet lake and a DuckDB warehouse
synchronized from a simulated transactional order-management source, using
Kafka as the transport and Spark Structured Streaming as the processing
layer. It covers all six of the assignment's core requirements: a source
schema with real strong/weak entities, a CDC flow that captures every
insert/update/delete, fail-closed schema-drift detection, point-in-time
restore via a bounded lake replay, a validation-parity layer that mirrors
the source's constraints against the (deliberately loosely-typed)
warehouse, and a catalog exposing every lake/warehouse dataset with
ownership and consumer metadata.

I initially planned this with Delta Lake as the warehouse storage layer
(see the very first version of this document, still visible in the git
history / the early PR commits). During implementation I simplified that
to plain DuckDB plus an explicit `reconstruct_as_of()` replay function
instead — this avoided taking on a table-format dependency for a
single-node, assignment-scoped warehouse, at the cost of not getting
Delta's built-in `VERSION AS OF` querying for free. I'd rather be upfront
about that change than leave the original plan looking like what actually
got built.

I also want to flag early, rather than bury it: this is *simulated* CDC.
DuckDB has no WAL or logical-replication slot the way Postgres does, so
there's no real log to tail. Change capture happens at the application
layer instead — documented in detail below, including the specific
failure mode this introduces and how a real Postgres-backed deployment
would close that gap.

This document is the design narrative. For quicker reference: `feature.md`
maps each requirement below to exactly what got built, `source.md` has the
full schema and every invariant, and `docs/` has one file per component
(CDC/streaming, warehouse/time-travel, schema/data-quality, catalog) with
implementation-level detail I didn't want to bloat this document with.
`README.md` has the setup and run commands.

## Source Schema Design

Domain: order-management, e-commerce. Customers place orders; orders
contain line items and are paid via one or more payment attempts.

**Strong entities**: `customers`, `orders` — both have independent
identity and their own lifecycle, not owned by anything else.

**Weak entities**: `order_items` (its lifecycle is entirely tied to its
parent order — a line item has no meaning outside an order) and
`payments` (tied to an order, but more loosely — a payment can arrive or
retry after the order was created, so it carries its own timestamps and
status independent of the order's).

```
customers(customer_id PK, name, email, status enum, created_at, updated_at)
orders(order_id PK, customer_id FK -> customers, status enum,
       total_amount DECIMAL(18,2), discount_code nullable,
       created_at, updated_at)
order_items(order_item_id PK, order_id, sku, quantity INT >0,
            unit_price DECIMAL(18,2) >=0)
payments(payment_id PK, order_id, amount DECIMAL(18,2) >0, status enum,
         created_at, paid_at nullable)
```

Indexes: `idx_orders_customer_id`, `idx_orders_status`,
`idx_order_items_order_id`, `idx_payments_order_id` — on every FK-shaped
lookup column, since those are what both the referential-integrity check
and any downstream analytical query would filter/join on.

**One deliberate deviation from a textbook FK design**: `order_items.order_id`
and `payments.order_id` are *not* declared with `REFERENCES orders(order_id)`,
even though that's the obviously "correct" relational choice. I hit a real
DuckDB engine limitation while building this — on DuckDB 1.5.5, you cannot
`UPDATE` a table that is simultaneously an FK child (`orders` references
`customers`) and an FK parent (`order_items`/`payments` reference `orders`):
any `UPDATE` on `orders` fails with "Violates foreign key constraint
because key is still referenced," even when the update doesn't touch
`order_id` at all. I didn't take this at face value — I built an isolated
minimal repro (a plain grandparent → parent → child FK chain, nothing else
in it) before concluding it was a genuine engine bug rather than something
wrong in my schema. Since `orders.status` changes constantly (the single
most common write in this domain), `orders` had to stay updatable, so I
dropped the FK declaration on the child side and moved that referential
check into the data-quality layer instead. On Postgres or MySQL this
workaround wouldn't be necessary.

**Validation rules** (source-enforced via PK/CHECK/NOT NULL, re-verified
downstream — see Validation Parity):
- `orders.total_amount` should reconcile with
  `sum(order_items.quantity * unit_price)` for that order.
- `order_items.quantity > 0`, `unit_price >= 0`; `payments.amount > 0`.
- `payments.paid_at >= payments.created_at` when set.
- Status fields restricted to a known enum per table.
- Status transitions follow a legal state machine (e.g. an order can go
  `pending -> paid -> shipped`, but not `cancelled -> paid`).

## CDC Strategy

**How changes are captured**: a `SourceWriter` class does two things, in
this order, on every call: (1) runs the real SQL against `source.duckdb`
so the source's own constraints apply, and (2) — only after that succeeds
— publishes the corresponding event to Kafka. This is application-level
dual-write, not log-based capture, because DuckDB doesn't expose a WAL to
tail. I'm calling this out plainly rather than dressing it up: if the
process crashes between step 1 and step 2, that change is durably in the
source but was never published. In production, with a real Postgres
source, this would be replaced by Debezium tailing the database's WAL,
which guarantees capture regardless of what happens to any application
process in between.

**How inserts/updates/deletes are handled**: all three map to a Kafka
event carrying an `operation` field (`insert`/`update`/`delete`), the
table name, the primary key, a full snapshot of the row's current values,
and a capture timestamp. Deletes are captured with the row's last-known
values (not just the key) so the lake retains what the row looked like
right before deletion.

**Ordering**: the Kafka topic (`cdc-events-v4`) has exactly one partition,
so the message offset is a true global order across every table — that
offset becomes the `sequence` number used by every downstream component
(lake row, warehouse `_cdc_seq`, time-travel bounding), with no separate
manually-maintained counter. I chose durability confirmation over
throughput here: the producer uses `acks="all"` and blocks
(`future.get(timeout=10)`) until the broker fully acknowledges the write,
so a successful call is a real durability guarantee, not just "sent."

**How replay/restart works**: Spark Structured Streaming's own
`checkpointLocation` tracks which Kafka offsets have already been
processed — that's the restart-safety mechanism, built into the framework
itself, so I didn't need a separate offset-tracking table for this path.
I'm relying on this being a well-established Spark guarantee rather than
having independently proven it here: I haven't run a dedicated
kill-and-restart test against this exact pipeline, and there's no
automated test for it either (see Known Limitations) — the `_cdc_seq`
guard in the warehouse merge is a second, independent safety net either
way, so a redelivered batch is a no-op regardless.

**How duplicates are handled**: the warehouse merge upserts with
`WHERE excluded._cdc_seq > current._cdc_seq` — a duplicate or
out-of-order event can never overwrite a row that's already ahead of it,
so replaying the same batch twice is a no-op on the warehouse. The lake
itself isn't strictly exactly-once at the storage layer — I actually hit
this during testing: a Spark batch that crashed mid-way (after writing to
Parquet but before finishing the warehouse write) produced a duplicate row
in the lake once I reran it. Harmless for warehouse correctness, since the
merge guard dedupes regardless of how many times an event is replayed, but
worth being honest that the lake can carry small amounts of redundant
storage as a result.

## Lake and Warehouse Modeling

**Lake**: append-only Parquet, written by Spark's native streaming sink,
partitioned by day (`data/lake_parquet/event_date=YYYY-MM-DD/`) so a
specific day's history can be targeted directly for reprocessing,
investigation, or a bounded restore without scanning the whole lake.
Nothing here is ever updated or deleted — it's the durable, replayable
history everything else is built from.

**Warehouse**: DuckDB tables (`wh_customers`, `wh_orders`, `wh_order_items`,
`wh_payments`), each mirroring its source table's columns plus `_cdc_seq`
(sequence of the last applied event) and `_deleted` (soft-delete flag — a
source delete never physically removes the warehouse row, since it's
still needed for history/restore). There's no separate write path into
the warehouse; it's always a function of "which lake events have been
applied so far," which is what keeps it consistent with the lake and what
makes rebuild/restore possible in the first place.

**Time travel / restore**: `reconstruct_as_of(lake_rows_up_to_bound)`
takes a caller-bounded list of lake events (bounded by sequence or by
timestamp — the function itself doesn't care which), replays them through
the exact same merge logic into a brand-new, throwaway, in-memory
warehouse, and returns that connection. It never touches the live
warehouse — "restore" here means rebuild elsewhere, verify it, then decide
what to do with the live system, not overwrite it blind.

I verified this live, not just in unit tests: for one order that moved
`pending -> paid -> shipped` across three real Kafka offsets (19, 20, 21),
bounding the replay to `sequence <= 19` correctly reproduced `pending`,
bounding to `sequence <= 20` correctly reproduced `paid`, and the live
warehouse correctly showed `shipped` — three different, independently
correct answers for three different bounds, none of which touched the
live warehouse.

## Schema Change Safety

`check_schema_contracts.py` diffs the live source's
`information_schema.columns` against an expected-columns/types contract.
A **breaking** change (dropped column, renamed column, changed type)
raises `SchemaContractViolation`; the script catches it at the top level,
prints every specific problem, and exits non-zero — ingestion should stop
rather than keep writing against a wrong assumption about the schema. A
purely **additive** change (a genuinely new column) is reported as a
warning instead of a failure, since old code simply won't reference a
column it doesn't know about yet — it's a signal the contract needs
updating, not a reason to halt.

I'll be direct about the current gap here: nothing automated is watching
for that non-zero exit code today — it's a manually-run script. In
production this would be wired into an orchestrator (Airflow) so a
schema-contract failure fails the task and pages someone, or pauses the
pipeline until it's fixed.

## Validation Parity

The warehouse tables are typed loosely on purpose, so the merge/upsert
logic can stay generic across all four tables — which means none of the
source's constraints (`NOT NULL`, `CHECK`, the one FK) physically exist on
the warehouse side. `run_data_quality_checks.py` puts that parity back:

- **System checks**: PK uniqueness, not-null, referential integrity
  (including the `order_items`/`payments` → `orders` link that DuckDB
  itself couldn't enforce), enum domain.
- **Business checks**: non-negative amounts, order-total-matches-line-items,
  payment timing, and status-transition legality. The last one needs the
  lake's *full* history per entity, not just the warehouse's latest state
  — detecting an illegal transition (`cancelled -> paid`) requires knowing
  the sequence of past statuses, which the warehouse structurally doesn't
  retain.

Failures are surfaced the same way as the schema check: an `enforce()`
function raises `DataQualityViolation` listing every failing category and
row, caught at the top level, non-zero exit. Same current gap as above —
nothing automated pages anyone on failure yet.

## Catalog Exposure

`catalog/catalog.json` registers 5 datasets: the lake dataset
(`lake_cdc_events`) and the four warehouse tables. There's no
raw/source-layer entry — the source database is the transactional system
of record, not something published for downstream consumption. Each entry
documents an owner, its intended consumers (these genuinely differ per
table — `wh_payments` lists `finance` and `audit`, `wh_order_items`
doesn't), an update cadence, a description, and the full column schema.

This isn't just a document that can silently go stale: `validate_catalog.py`
checks that every dataset the pipeline actually produces has a complete
catalog entry, and that a warehouse entry's documented schema hasn't
drifted from what the code actually expects. A catalog that can drift
unnoticed is worse than no catalog, so I made that check automatic rather
than something to remember.

A consumer discovers datasets by reading `catalog.json` directly (or a
production metadata platform indexing it — see Known Limitations) and
queries them accordingly: the lake via `read_parquet('data/lake_parquet/*/*.parquet')`
from any DuckDB connection, the warehouse by connecting directly to
`warehouse.duckdb` and querying `wh_*` tables with normal SQL.

## How to Run / Validate

```bash
# tests (in-memory, ~2s, never touches real source/warehouse data)
python -m pytest -q

# source schema still matches the expected contract
python scripts/check_schema_contracts.py

# warehouse + lake pass every system and business validation
python scripts/run_data_quality_checks.py

# every lake/warehouse dataset is registered with complete metadata
python scripts/validate_catalog.py

# seed a consistent demo dataset — writes to source.duckdb AND publishes
# the matching events to Kafka in the same run
python scripts/seed_demo_data.py

# process those Kafka events into the lake + warehouse (on-demand, default —
# processes whatever's currently in Kafka, then exits)
docker run --rm --network host --user 1000:1000 \
  -v /etc/passwd:/etc/passwd:ro \
  -v /tmp/kafka-clients-3.9.0.jar:/opt/spark/jars/kafka-clients-4.0.0.jar:ro \
  -v /tmp/commons-pool2-2.12.0.jar:/opt/spark/jars/commons-pool2-2.12.0.jar:ro \
  -v /u01/container-pylibs:/opt/pylibs:ro \
  -v $(pwd):/workspace -w /workspace -e PYTHONPATH=/opt/pylibs \
  spark-kafka:4.0.0 /opt/spark/bin/spark-submit --master 'local[*]' pipeline/spark_consumer.py

# or, to run continuously instead (polls Kafka every 5 seconds and keeps
# picking up new events automatically — stop with `docker stop` when done,
# it holds a resident Spark driver process):
docker run --rm --network host --user 1000:1000 \
  -v /etc/passwd:/etc/passwd:ro \
  -v /tmp/kafka-clients-3.9.0.jar:/opt/spark/jars/kafka-clients-4.0.0.jar:ro \
  -v /tmp/commons-pool2-2.12.0.jar:/opt/spark/jars/commons-pool2-2.12.0.jar:ro \
  -v /u01/container-pylibs:/opt/pylibs:ro \
  -v $(pwd):/workspace -w /workspace -e PYTHONPATH=/opt/pylibs \
  spark-kafka:4.0.0 /opt/spark/bin/spark-submit --master 'local[*]' pipeline/spark_consumer.py --continuous
```

I've run every command above, and the full end-to-end flow (seed data →
Kafka → Spark → lake + warehouse), against this exact code: 21/21 tests
pass, schema contract passes, data quality checks pass with zero
violations, and catalog validation passes.

## Known Limitations / Next Steps

Things I'd tackle next, in priority order, if this were going to
production:

1. **Real CDC, not simulated** — replace the application-level dual-write
   with Debezium tailing a real Postgres WAL, closing the crash-between-writes
   gap described above.
2. **Monitoring and alerting** — right now, a schema-contract or
   data-quality failure just exits non-zero; nothing pages anyone. I'd add
   Kafka consumer-lag metrics, Spark `StreamingQueryListener` batch-lag
   metrics, and wire validation failures into Slack/PagerDuty.
3. **Orchestration** — Spark currently runs via a manual Docker command; a
   production setup needs Airflow (or similar) for scheduling and
   restart-on-failure, especially for the on-demand run mode.
4. **Multi-partition Kafka** — a single partition is a throughput ceiling
   at real volume. The fix is partitioning by entity key and relying on
   the `_cdc_seq` guard for per-row order instead of a global order.
5. **A concurrent-capable production warehouse** — DuckDB is single-writer,
   which is fine for this scale and for proving correctness, but I
   wouldn't choose it for a production warehouse serving concurrent
   analysts while Spark is also writing to it.
6. **Richer schema-evolution handling** — the current contract check only
   distinguishes breaking vs. additive; a schema-registry-based
   compatibility model (Avro/Protobuf-style) would handle more nuance.
7. **No automated restart-recovery test** — restart-safety currently rests
   on Spark's own checkpointing (a well-established framework guarantee)
   plus the warehouse's `_cdc_seq` guard, but I don't have a dedicated
   kill-and-restart test proving it against this exact pipeline. This is
   the most concrete gap I'd close first, since the assignment's testing
   requirements call for it directly.

## Responsible AI Usage

I used Claude during implementation to speed up writing boilerplate and
draft code. The domain choice, the build order (a DuckDB-only core first
to validate the merge logic, then Kafka and Spark layered on top), and the
trade-offs (DuckDB over Postgres, on-demand as the default run mode given
the VM's memory constraints) were my decisions. I personally ran and
verified the infrastructure at every stage — records landing in
Kafka/lake/warehouse, continuous mode picking up a new record live, and
the time-travel reconstruction documented above.
