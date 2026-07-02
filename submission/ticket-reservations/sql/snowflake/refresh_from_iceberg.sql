-- Snowflake refresh after each successful 5-minute Spark micro-batch.
-- In production these SELECTs point at Snowflake-managed Iceberg tables or
-- external Iceberg tables registered against the S3 lakehouse catalog.

MERGE INTO WH_TICKETING_CURRENT.RESERVATIONS_CURRENT AS t
USING EXT_SILVER.RESERVATIONS_CURRENT AS s
ON t.reservation_id = s.reservation_id
WHEN MATCHED AND s.source_lsn > t.source_lsn THEN UPDATE SET
  customer_id = s.customer_id,
  event_id = s.event_id,
  status = s.status,
  hold_expires_at = s.hold_expires_at,
  confirmed_at = s.confirmed_at,
  total_amount = s.total_amount,
  currency = s.currency,
  created_at = s.created_at,
  updated_at = s.updated_at,
  is_deleted = s.is_deleted,
  source_lsn = s.source_lsn,
  source_event_at = s.source_event_at,
  warehouse_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT VALUES (
  s.reservation_id, s.customer_id, s.event_id, s.status, s.hold_expires_at,
  s.confirmed_at, s.total_amount, s.currency, s.created_at, s.updated_at,
  s.is_deleted, s.source_lsn, s.source_event_at, CURRENT_TIMESTAMP()
);

MERGE INTO WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT AS t
USING EXT_SILVER.RESERVATION_ITEMS_CURRENT AS s
ON t.reservation_item_id = s.reservation_item_id
WHEN MATCHED AND s.source_lsn > t.source_lsn THEN UPDATE SET
  reservation_id = s.reservation_id,
  seat_id = s.seat_id,
  unit_price = s.unit_price,
  service_fee = s.service_fee,
  currency = s.currency,
  created_at = s.created_at,
  is_deleted = s.is_deleted,
  source_lsn = s.source_lsn,
  source_event_at = s.source_event_at,
  warehouse_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT VALUES (
  s.reservation_item_id, s.reservation_id, s.seat_id, s.unit_price,
  s.service_fee, s.currency, s.created_at, s.is_deleted, s.source_lsn,
  s.source_event_at, CURRENT_TIMESTAMP()
);

MERGE INTO WH_TICKETING_CURRENT.PAYMENTS_CURRENT AS t
USING EXT_SILVER.PAYMENTS_CURRENT AS s
ON t.payment_id = s.payment_id
WHEN MATCHED AND s.source_lsn > t.source_lsn THEN UPDATE SET
  reservation_id = s.reservation_id,
  provider = s.provider,
  provider_reference = s.provider_reference,
  status = s.status,
  amount = s.amount,
  currency = s.currency,
  authorized_at = s.authorized_at,
  captured_at = s.captured_at,
  refunded_at = s.refunded_at,
  created_at = s.created_at,
  is_deleted = s.is_deleted,
  source_lsn = s.source_lsn,
  source_event_at = s.source_event_at,
  warehouse_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT VALUES (
  s.payment_id, s.reservation_id, s.provider, s.provider_reference, s.status,
  s.amount, s.currency, s.authorized_at, s.captured_at, s.refunded_at,
  s.created_at, s.is_deleted, s.source_lsn, s.source_event_at,
  CURRENT_TIMESTAMP()
);
