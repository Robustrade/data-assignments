# Ticketing Reservations CDC Lakehouse

This submission shows a small CDC pipeline for a ticketing and reservations system.

Stack:

- PostgreSQL source schema
- Debezium CDC into Kafka
- raw Kafka events landed in S3
- Spark Structured Streaming on a 5-minute trigger
- Apache Iceberg tables in S3 for the silver layer
- Snowflake refresh from silver Iceberg tables into current/history tables

## Data Flow

```text
PostgreSQL
  -> Debezium WAL CDC
  -> Kafka topics
  -> raw Kafka event files in S3
  -> Spark Structured Streaming every 5 minutes
  -> Iceberg silver tables in S3
       silver.ticketing_cdc_events
       silver.reservations_current
       silver.reservation_items_current
       silver.payments_current
       silver.reservations_history
  -> Snowflake refresh
       WH_TICKETING_CURRENT.RESERVATIONS_CURRENT
       WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT
       WH_TICKETING_CURRENT.PAYMENTS_CURRENT
       WH_TICKETING_HISTORY.RESERVATIONS_HISTORY
```

Checkpoint path:

```text
s3://ticketing-lakehouse/checkpoints/raw-s3-cdc-to-iceberg-5min
```

Offsets and recovery:

- Spark advances offsets only after a successful micro-batch.
- If the job fails, the same raw S3 files are read again from the last checkpoint.
- Duplicate events are ignored by deterministic `event_id`.
- Late events are handled by `source_lsn` ordering in the current tables.

## Files

- `source/models.py`: source schema and contract
- `pipeline/lake.py`: local silver-table writer used for tests
- `pipeline/warehouse.py`: local warehouse current/history writer
- `pipeline/schema_contracts.py`: incompatible schema detection
- `sql/source/postgres_schema.sql`: source DDL
- `sql/source/debezium_connector.json`: CDC connector config
- `sql/spark/raw_s3_event_contract.sql`: raw S3 event file shape
- `sql/spark/iceberg_tables.sql`: silver Iceberg tables
- `sql/spark/structured_streaming_pseudocode.py`: 5-minute Spark flow
- `sql/snowflake/*.sql`: Snowflake refresh and quality SQL
- `catalog/catalog.json`: dataset metadata

## Run

```bash
cd submission/ticket-reservations
python -m pip install -r requirements.txt
pytest
python scripts/check_schema_contracts.py
python scripts/validate_catalog.py
python scripts/run_data_quality_checks.py
```

For behavior-focused local testing steps, see `LOCAL_VALIDATION.md`.
