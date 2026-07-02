-- Raw Kafka CDC events are already landed in S3 by the time this pipeline starts.
-- Example path layout:
--   s3://ticketing-raw-cdc/events/source_table=reservations/ingest_date=2026-07-02/*.json
--
-- Each file row is one Kafka CDC message. The pipeline reads these files as a
-- Spark Structured Streaming file source and writes durable Iceberg tables.

CREATE DATABASE IF NOT EXISTS raw_contract;

CREATE TABLE IF NOT EXISTS raw_contract.kafka_cdc_event_shape (
  event_id STRING,
  source_table STRING,
  primary_key_json STRING,
  op STRING,
  before_json STRING,
  after_json STRING,
  schema_id STRING,
  source_lsn BIGINT,
  source_tx_id STRING,
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  source_event_at TIMESTAMP,
  landed_at TIMESTAMP
);
