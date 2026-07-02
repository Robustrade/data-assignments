CREATE SCHEMA IF NOT EXISTS WH_TICKETING_CURRENT;
CREATE SCHEMA IF NOT EXISTS WH_TICKETING_HISTORY;
CREATE SCHEMA IF NOT EXISTS QA;
CREATE SCHEMA IF NOT EXISTS OPS;

CREATE TABLE IF NOT EXISTS WH_TICKETING_CURRENT.RESERVATIONS_CURRENT (
  reservation_id STRING PRIMARY KEY,
  customer_id STRING,
  event_id STRING,
  status STRING,
  hold_expires_at TIMESTAMP_TZ,
  confirmed_at TIMESTAMP_TZ,
  total_amount NUMBER(12,2),
  currency STRING,
  created_at TIMESTAMP_TZ,
  updated_at TIMESTAMP_TZ,
  is_deleted BOOLEAN,
  source_lsn NUMBER,
  source_event_at TIMESTAMP_TZ,
  warehouse_updated_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT (
  reservation_item_id STRING PRIMARY KEY,
  reservation_id STRING,
  seat_id STRING,
  unit_price NUMBER(12,2),
  service_fee NUMBER(12,2),
  currency STRING,
  created_at TIMESTAMP_TZ,
  is_deleted BOOLEAN,
  source_lsn NUMBER,
  source_event_at TIMESTAMP_TZ,
  warehouse_updated_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS WH_TICKETING_CURRENT.PAYMENTS_CURRENT (
  payment_id STRING PRIMARY KEY,
  reservation_id STRING,
  provider STRING,
  provider_reference STRING,
  status STRING,
  amount NUMBER(12,2),
  currency STRING,
  authorized_at TIMESTAMP_TZ,
  captured_at TIMESTAMP_TZ,
  refunded_at TIMESTAMP_TZ,
  created_at TIMESTAMP_TZ,
  is_deleted BOOLEAN,
  source_lsn NUMBER,
  source_event_at TIMESTAMP_TZ,
  warehouse_updated_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS WH_TICKETING_HISTORY.RESERVATIONS_HISTORY (
  reservation_id STRING,
  customer_id STRING,
  event_id STRING,
  status STRING,
  hold_expires_at TIMESTAMP_TZ,
  confirmed_at TIMESTAMP_TZ,
  total_amount NUMBER(12,2),
  currency STRING,
  created_at TIMESTAMP_TZ,
  updated_at TIMESTAMP_TZ,
  valid_from TIMESTAMP_TZ,
  valid_to TIMESTAMP_TZ,
  is_current BOOLEAN,
  is_deleted BOOLEAN,
  source_lsn NUMBER,
  event_id_cdc STRING PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS OPS.SCHEMA_FAILURES (
  failure_id STRING PRIMARY KEY,
  source_table STRING,
  expected_schema VARIANT,
  received_schema VARIANT,
  kafka_topic STRING,
  kafka_partition NUMBER,
  kafka_offset NUMBER,
  failed_at TIMESTAMP_TZ,
  message STRING
);

COMMENT ON TABLE WH_TICKETING_CURRENT.RESERVATIONS_CURRENT IS
  'Latest reservation state from PostgreSQL CDC. Deletes are soft-deleted by is_deleted.';

COMMENT ON TABLE WH_TICKETING_HISTORY.RESERVATIONS_HISTORY IS
  'SCD2 reservation history rebuilt from immutable Iceberg CDC events.';
