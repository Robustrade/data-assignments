MERGE INTO WH_TICKETING_CURRENT.RESERVATIONS_CURRENT AS target
USING STAGE_RESERVATIONS_CDC AS source
ON target.reservation_id = source.reservation_id
WHEN MATCHED AND source.source_lsn > target.source_lsn THEN UPDATE SET
  customer_id = source.customer_id,
  event_id = source.event_id,
  status = source.status,
  hold_expires_at = source.hold_expires_at,
  confirmed_at = source.confirmed_at,
  total_amount = source.total_amount,
  currency = source.currency,
  created_at = source.created_at,
  updated_at = source.updated_at,
  is_deleted = source.is_deleted,
  source_lsn = source.source_lsn,
  source_event_at = source.source_event_at,
  warehouse_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  reservation_id, customer_id, event_id, status, hold_expires_at, confirmed_at,
  total_amount, currency, created_at, updated_at, is_deleted, source_lsn,
  source_event_at, warehouse_updated_at
) VALUES (
  source.reservation_id, source.customer_id, source.event_id, source.status,
  source.hold_expires_at, source.confirmed_at, source.total_amount,
  source.currency, source.created_at, source.updated_at, source.is_deleted,
  source.source_lsn, source.source_event_at, CURRENT_TIMESTAMP()
);

