-- Mirrors the source CHECK: only SETTLED or REFUNDED payments carry a
-- settlement timestamp.
select
    payment_id,
    status,
    settled_at
from {{ ref("payment_current") }}
where settled_at is not null
  and status not in ('SETTLED', 'REFUNDED')
