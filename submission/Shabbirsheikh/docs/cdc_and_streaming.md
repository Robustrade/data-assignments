# CDC Capture and Streaming

Covers: `pipeline/source_writer.py`, `pipeline/kafka_producer.py`,
`pipeline/spark_consumer.py`.

## Capture: application-level dual-write, not log-based

`SourceWriter.insert/update/delete()` does two things in sequence, on
every call:

1. Runs the real SQL against `source.duckdb` (so the source's own
   PK/FK/CHECK/NOT NULL constraints apply, same as production).
2. Only *after* that succeeds, calls `KafkaChangeCapture.emit()` to
   publish the matching event to Kafka.

This ordering matters: it guarantees the pipeline never emits an event for
something that didn't actually happen in the source.

**This is simulated CDC, not log-based capture.** DuckDB has no
WAL / logical-replication slot to tail the way Postgres does, so there is
no way to do real log-based capture against it. The known risk: if the
process crashes between step 1 and step 2, that change is durably in the
source but was never published — the classic "dual-write problem." In
production, with a real Postgres (or similar) source, this would be
replaced by Debezium tailing the database's WAL, which guarantees that
whatever commits to the source is captured regardless of what happens to
any application process.

## Transport: Kafka, single partition

`pipeline/kafka_producer.py`:

- `make_producer()` creates a `KafkaProducer` with `acks="all"` — the
  `.send()` call blocks (`future.get(timeout=10)`) until the broker
  fully acknowledges the write. That acknowledgment is the durability
  point: once it returns, the event is durable on the broker even if
  every downstream consumer is down.
- Topic `cdc-events-v4` is created with exactly one partition. This means
  the Kafka message offset is a true global ordering across every event
  from every table — that offset becomes the `sequence` number used by
  every downstream component (lake row, warehouse `_cdc_seq`, time-travel
  bounding), with no separate manually-maintained counter.
- Trade-off: a single partition caps throughput to what one partition can
  handle. At real production volume, the fix is to partition by entity
  key (e.g. `customer_id`) and drop the requirement for *global* order —
  only *per-row* order matters for correctness, which the warehouse's
  `_cdc_seq` guard already enforces independent of partition count.

## Processing: Spark Structured Streaming

`pipeline/spark_consumer.py`:

- Reads the topic (`readStream.format("kafka")`), parses the JSON
  envelope (`operation`, `table_name`, `primary_key`, `data`,
  `captured_at`), and derives an `event_date` partition column from
  `captured_at`.
- `foreachBatch(process_batch)` does two things per micro-batch:
  1. Appends the batch to the Parquet lake
     (`data/lake_parquet/event_date=YYYY-MM-DD/*.parquet`), partitioned by
     day so a specific day's history can be targeted directly later
     (reprocessing, investigation, a bounded restore) without scanning
     the whole lake.
  2. Collects the batch to the driver, in sequence order, and hands it to
     `pipeline/warehouse.py`'s `apply_lake_events()` — the same merge
     function the test suite exercises directly. Spark's job is only "get
     Kafka data to the driver, in order"; the correctness guarantee
     (last-write-wins via `_cdc_seq`) comes from code that's independently
     tested.
- The DuckDB warehouse connection is opened and closed fresh **inside**
  `process_batch()`, per batch — not held for the job's lifetime. This
  was a deliberate fix after hitting a real cross-process test failure:
  DuckDB holds an exclusive lock for as long as a connection stays open,
  so in continuous mode (which can run for hours), a long-lived
  connection would permanently lock out every other reader. Reconnecting
  per batch means the lock is only held for the few milliseconds it takes
  to apply that batch.

## Run modes and latency

- **On-demand (default)**: `trigger(availableNow=True)` — processes
  whatever is currently in Kafka, then exits. Predictable, no background
  process left running.
- **Continuous** (`--continuous` flag): `trigger(processingTime="5
  seconds")` — polls Kafka every 5 seconds indefinitely. Worst-case
  latency for a single event is bounded by that interval (up to 5
  seconds); best case, an event arriving just before a scheduled poll is
  picked up almost immediately. This leaves a resident Spark driver
  process (JVM, a few hundred MB) — verified live that this needs to be
  explicitly stopped after use, since an unattended continuous container
  combined with a shared checkpoint directory across two different topic
  names caused real memory pressure and a corrupted checkpoint during
  development.

## Restart-safety

Spark's own `checkpointLocation` (`data/spark_checkpoints/main`) tracks
which Kafka offsets have already been processed — this is the
restart-safety mechanism for this path, built into Structured Streaming
itself. `apply_lake_events()`'s own `_cdc_seq` guard is a second,
independent safety net: even if Spark ever redelivered an already-applied
batch, re-applying it is a no-op on the warehouse.
