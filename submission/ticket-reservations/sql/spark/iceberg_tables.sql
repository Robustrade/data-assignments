-- Iceberg lakehouse tables written by Spark Structured Streaming to S3.
-- Catalog examples:
--   spark.sql.catalog.ticketing=org.apache.iceberg.spark.SparkCatalog
--   spark.sql.catalog.ticketing.type=hive
--   spark.sql.catalog.ticketing.warehouse=s3://ticketing-lakehouse/warehouse

CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS ops;

-- Silver: immutable CDC event history and typed current/history models.
CREATE TABLE IF NOT EXISTS silver.ticketing_cdc_events (
  event_id STRING,
  source_system STRING,
  source_database STRING,
  source_schema STRING,
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
  ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(source_event_at), source_table);

CREATE TABLE IF NOT EXISTS silver.reservations_current (
  reservation_id STRING,
  customer_id STRING,
  event_id STRING,
  status STRING,
  hold_expires_at TIMESTAMP,
  confirmed_at TIMESTAMP,
  total_amount DECIMAL(12,2),
  currency STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN,
  source_lsn BIGINT,
  source_event_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, reservation_id));

CREATE TABLE IF NOT EXISTS silver.reservation_items_current (
  reservation_item_id STRING,
  reservation_id STRING,
  seat_id STRING,
  unit_price DECIMAL(12,2),
  service_fee DECIMAL(12,2),
  currency STRING,
  created_at TIMESTAMP,
  is_deleted BOOLEAN,
  source_lsn BIGINT,
  source_event_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, reservation_id));

CREATE TABLE IF NOT EXISTS silver.payments_current (
  payment_id STRING,
  reservation_id STRING,
  provider STRING,
  provider_reference STRING,
  status STRING,
  amount DECIMAL(12,2),
  currency STRING,
  authorized_at TIMESTAMP,
  captured_at TIMESTAMP,
  refunded_at TIMESTAMP,
  created_at TIMESTAMP,
  is_deleted BOOLEAN,
  source_lsn BIGINT,
  source_event_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, reservation_id));

-- Silver history: SCD2 row versions used for restore and point-in-time queries.
CREATE TABLE IF NOT EXISTS silver.reservations_history (
  reservation_id STRING,
  customer_id STRING,
  event_id STRING,
  status STRING,
  hold_expires_at TIMESTAMP,
  confirmed_at TIMESTAMP,
  total_amount DECIMAL(12,2),
  currency STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  is_deleted BOOLEAN,
  source_lsn BIGINT,
  cdc_event_id STRING
)
USING iceberg
PARTITIONED BY (days(valid_from), bucket(16, event_id));

CREATE TABLE IF NOT EXISTS ops.schema_failures (
  failure_id STRING,
  source_table STRING,
  expected_schema_json STRING,
  received_schema_json STRING,
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  failed_at TIMESTAMP,
  message STRING
)
USING iceberg
PARTITIONED BY (days(failed_at), source_table);
