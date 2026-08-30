-- Mirrors two source CHECKs at once: a cancellation timestamp belongs only to
-- a CANCELLED order, and can never precede the order being placed.
select
    order_id,
    order_status,
    placed_at,
    cancelled_at
from {{ ref("mart_order_financials") }}
where (cancelled_at is not null and order_status <> 'CANCELLED')
   or (cancelled_at is not null and cancelled_at < placed_at)
