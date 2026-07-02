CREATE OR REPLACE VIEW QA.RESERVATION_TOTAL_MISMATCHES AS
SELECT
  r.reservation_id,
  r.total_amount,
  SUM(i.unit_price + i.service_fee) AS item_total
FROM WH_TICKETING_CURRENT.RESERVATIONS_CURRENT r
JOIN WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT i
  ON r.reservation_id = i.reservation_id
WHERE r.is_deleted = false
  AND i.is_deleted = false
GROUP BY r.reservation_id, r.total_amount
HAVING r.total_amount <> SUM(i.unit_price + i.service_fee);

CREATE OR REPLACE VIEW QA.CONFIRMED_RESERVATIONS_WITHOUT_CAPTURED_PAYMENT AS
SELECT r.reservation_id
FROM WH_TICKETING_CURRENT.RESERVATIONS_CURRENT r
LEFT JOIN WH_TICKETING_CURRENT.PAYMENTS_CURRENT p
  ON r.reservation_id = p.reservation_id
 AND p.status = 'captured'
 AND p.is_deleted = false
WHERE r.status = 'confirmed'
  AND r.is_deleted = false
  AND p.payment_id IS NULL;

CREATE OR REPLACE VIEW QA.SCD2_OVERLAPS AS
SELECT reservation_id, COUNT(*) AS overlapping_rows
FROM WH_TICKETING_HISTORY.RESERVATIONS_HISTORY h1
JOIN WH_TICKETING_HISTORY.RESERVATIONS_HISTORY h2
  ON h1.reservation_id = h2.reservation_id
 AND h1.event_id_cdc <> h2.event_id_cdc
 AND h1.valid_from < COALESCE(h2.valid_to, '9999-12-31'::TIMESTAMP_TZ)
 AND h2.valid_from < COALESCE(h1.valid_to, '9999-12-31'::TIMESTAMP_TZ)
GROUP BY reservation_id
HAVING COUNT(*) > 0;
