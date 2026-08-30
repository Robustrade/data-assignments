-- Mirrors the source CHECK (settled_at IS NULL OR settled_at >= created_at).
select
    payment_id,
    created_at,
    settled_at
from {{ ref("payment_current") }}
where settled_at is not null
  and settled_at < created_at
