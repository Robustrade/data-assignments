# Feature Specification — CDC Lakehouse Pipeline (E-commerce Orders)

This maps the assignment's six core requirements to what's actually built
and tested, not to an idealized version of it. `DESIGN.md` has the fuller
narrative and the reasoning behind each choice; this is the shorter,
requirement-by-requirement version.

## 1. Source Data Model

A transactional order-management schema — `customers`, `orders`,
`order_items`, `payments` — with a real strong/weak entity split and a
cross-table invariant (order total vs. line items) that's worth validating
downstream. Defined in `source/models.py` (`init_source_schema`), backed
by DuckDB with PK/FK/CHECK/NOT NULL constraints matching what a real OLTP
system would enforce. Full schema, entity rationale, and every invariant:
`source.md`.

## 2. CDC Pipeline

`pipeline/source_writer.py`'s `SourceWriter` writes the real SQL to
`source.duckdb` first, and only after that succeeds does it publish a
change event to Kafka via `pipeline/kafka_producer.py`. This is
application-level dual-write — simulated CDC, not log-based capture,
since DuckDB has no WAL or logical-replication slot to tail. The known
risk is a crash between the two writes losing an event; a production
source (Postgres) would use Debezium tailing the WAL instead, which
doesn't have that gap.

The Kafka topic (`cdc-events-v4`) has exactly one partition, so the
message offset is a true global order — that offset becomes the
`sequence` used everywhere downstream, with no separate manual counter.
`pipeline/spark_consumer.py` (Spark Structured Streaming) reads the topic,
writes every event to the Parquet lake, then feeds the batch to
`pipeline/warehouse.py`'s `apply_lake_events()`, which upserts the
warehouse with a `WHERE excluded._cdc_seq > current._cdc_seq` guard so a
duplicate or out-of-order replay can never overwrite a row that's already
ahead of it. It runs either on-demand (processes what's currently in
Kafka, then exits — the default) or continuously (polls every 5 seconds).
Full mechanics: `docs/cdc_and_streaming.md`.

## 3. Schema Change Detection and Safe Stop

`scripts/check_schema_contracts.py` diffs the live source's
`information_schema.columns` against an expected-columns/types contract
in `source/models.py`. A breaking difference — a dropped column, a
renamed column, a changed type — raises `SchemaContractViolation`, caught
at the top level and surfaced as a non-zero exit code listing every
specific problem. A purely additive change (a genuinely new column) is a
warning instead, since nothing downstream references it yet. Full
mechanics: `docs/schema_safety_and_data_quality.md`.

## 4. Historical Recovery and Time Travel

`pipeline/warehouse.py`'s `reconstruct_as_of(lake_rows_up_to_bound)` takes
a caller-bounded list of lake events (bounded by sequence or timestamp),
replays them through the same merge logic into a brand-new in-memory
DuckDB, and returns that connection without ever touching the live
warehouse. I verified this live rather than trusting the unit tests alone:
for an order that moved `pending -> paid -> shipped` across three real
Kafka offsets, bounding the replay to each earlier offset in turn
correctly reproduced `pending` and then `paid`, while the live warehouse
correctly showed `shipped`.

Worth noting: the original plan (see the early version of `DESIGN.md`)
proposed Delta Lake as the warehouse storage layer, using its native
version history for time travel. I simplified that to plain DuckDB plus
this explicit replay function during implementation, since it avoided a
table-format dependency for a warehouse this small — at the cost of not
getting Delta's built-in `VERSION AS OF` querying for free. Full
mechanics: `docs/warehouse_and_time_travel.md`.

## 5. Validation Parity

The warehouse tables are typed loosely, so none of the source's
constraints (PK uniqueness, not-null, referential integrity, enum domain)
physically exist on the warehouse side. `scripts/run_data_quality_checks.py`
re-checks them, plus the business rules — non-negative amounts, order
total vs. line items, payment timing, legal status transitions. The
status-transition check is the one that needs more than the warehouse's
latest state: detecting an illegal transition (`cancelled -> paid`)
requires the lake's full history per entity, since the warehouse has no
memory of what a status used to be. Full mechanics:
`docs/schema_safety_and_data_quality.md`.

## 6. Catalog Exposure

`catalog/catalog.json` registers 5 datasets — the lake plus the four
warehouse tables — each with an owner, intended consumers, update
cadence, and full column schema. `scripts/validate_catalog.py` enforces
that every dataset the pipeline actually produces has a complete entry,
and that a warehouse entry's documented schema hasn't drifted from the
code. Full details: `docs/catalog.md`.

## Reliability Behavior

Duplicate events are idempotent at the warehouse via the `_cdc_seq`
guard — a duplicate can still appear in the Parquet lake itself (e.g.
after a retried Spark batch), but that's harmless for warehouse
correctness. Out-of-order arrival is mostly prevented by the
single-partition Kafka topic, with the `_cdc_seq` guard as a second,
independent safety net either way. Restart safety rests on Spark's own
`checkpointLocation` tracking processed offsets, so there's no separate
offset-tracking table for this path. Deletes are soft — the warehouse row
is flagged `_deleted = TRUE` rather than removed, so history and restore
stay intact.

## Known Limitations (Not Yet Built)

- No orchestration — Spark runs manually via a Docker command, no Airflow
  scheduling or restart-on-failure.
- No monitoring or alerting — schema/data-quality failures currently just
  exit non-zero, nothing pages anyone.
- The lake isn't exactly-once at the storage layer — a retried batch can
  duplicate Parquet rows, though warehouse correctness is unaffected.
- Schema-evolution detection is basic — dropped/renamed/type-changed vs.
  additive only, no schema-registry-based compatibility negotiation.
- No automated restart-recovery test — restart safety relies on Spark's
  own checkpointing (a well-established framework guarantee), but there's
  no dedicated kill-and-restart test proving it against this exact
  pipeline.
