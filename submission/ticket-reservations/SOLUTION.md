# Ticketing Reservations CDC Lakehouse Solution

## Summary

This is a small CDC design for a ticketing and reservations platform. Raw Kafka CDC events are already landed in S3. Spark Structured Streaming reads those files every 5 minutes, validates the schema, writes the data to Iceberg silver tables in S3 and then refreshes Snowflake warehouse current/history tables.

## Source Model

The source model is intentionally centered on a reservation transaction rather than on analytics facts. The tables were chosen because they represent the durable business objects and lifecycle records needed to answer: what was offered, who reserved it, which seats were included, how it was paid for and which tickets were ultimately issued.

Strong entities:

- `venues`: stable place where events occur.
- `events`: independently scheduled inventory container for sellable seats.
- `customers`: independent buyer identity used across reservations.
- `reservations`: the main transaction root tying customer, event, status, money and hold/confirmation timing together.

Weak entities:

- `seats`: dependent on a venue for real-world identity; the natural key is venue + section + row + seat number.
- `reservation_items`: line items that depend on a reservation and seat.
- `payments`: payment attempts tied to a reservation lifecycle.
- `tickets`: issued fulfillment records tied one-to-one to reservation items.

Entity relationships:

- A venue has many events and many seats.
- An event belongs to one venue and has many reservations.
- A customer has many reservations.
- A reservation belongs to one customer and one event and has many reservation items and payments.
- A reservation item belongs to one reservation and one seat.
- A ticket belongs to one reservation item, event, and customer.

What makes a table strong vs weak in this model:

- Strong tables have independent lifecycle ownership and can be reasoned about without another row as their parent. For example, a customer or venue remains meaningful even if there are no reservations.
- Weak tables refine or fulfill a parent workflow and carry business meaning through that parent. For example, a reservation item is not useful without its reservation and a ticket is fulfillment of a specific purchased item.

Important rules:

- event end time must be after start time
- held reservations require `hold_expires_at`
- amounts and fees must be non-negative
- payment capture cannot predate authorization
- ticket use cannot predate issuance
- reservation item seats are unique within a reservation
- ticket codes and provider references are unique to prevent duplicate fulfillment or payment application
- reservation, payment, item and ticket currencies must agree for a single reservation workflow

Expected change patterns:

- `venues`, `customers` and `seats` are relatively stable, with occasional profile, contact or seat-status updates.
- `events` change during planning and sales, then become mostly immutable after closure or cancellation.
- `reservations` are highly mutable while held and settle into confirmed, expired, or cancelled terminal states.
- `reservation_items` are usually inserted during hold creation and rarely updated except for pricing corrections before confirmation.
- `payments` move through authorization, capture, failure and refund states, so updates are expected and ordering matters.
- `tickets` are created after confirmation and then change mainly for voiding or usage scan events.

## Flow

```text
raw Kafka event files in S3
  -> Spark Structured Streaming every 5 minutes
  -> Iceberg silver tables
  -> Snowflake current/history refresh
```

## Silver Layer

Silver tables in Iceberg:

- `silver.ticketing_cdc_events`
- `silver.reservations_current`
- `silver.reservation_items_current`
- `silver.payments_current`
- `silver.reservations_history`

`silver.ticketing_cdc_events` keeps every insert, update, and delete event. The current tables keep the latest row per business key. The history table stores SCD2 versions for point-in-time restore.

## Warehouse Layer

Snowflake tables:

- `WH_TICKETING_CURRENT.RESERVATIONS_CURRENT`
- `WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT`
- `WH_TICKETING_CURRENT.PAYMENTS_CURRENT`
- `WH_TICKETING_HISTORY.RESERVATIONS_HISTORY`

Snowflake refresh is idempotent and only applies newer `source_lsn` values. That keeps replay safe and prevents stale data from overwriting newer rows.

## Checkpointing

The Spark job uses a 5-minute trigger and stores offsets in:

```text
s3://ticketing-lakehouse/checkpoints/raw-s3-cdc-to-iceberg-5min
```

Why it helps:

- restart resumes from the last successful checkpoint
- duplicate delivery is ignored by `event_id`
- late events are ordered by `source_lsn`
- partial failure does not advance the checkpoint

## Schema Safety

Incoming rows are checked against the source contract before write.

Breaking changes stop ingestion:

- dropped columns
- type changes
- unknown tables
- key changes
- enum/domain contraction

On failure, the pipeline writes a record to `ops.schema_failures` and does not continue silently.

## Validation Parity

Warehouse checks mirror source rules:

- primary key uniqueness
- referential integrity
- domain checks
- reservation total checks
- payment timing checks
- SCD2 overlap checks

## Catalog

The catalog exposes:

- raw Kafka event files in S3
- silver Iceberg tables
- Snowflake current/history tables
- schema failure records

## Responsible AI Usage

AI helped draft the first version and organize the structure. The final design, naming and failure behavior were reviewed and simplified to match the requested pipeline shape.
