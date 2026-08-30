-- Mirrors the source CHECK: extended_amount = quantity * unit_price.
-- Restated here because the source stores money with NUMERIC affinity (float)
-- while the warehouse uses exact DECIMAL(18,2); if the cast on ingest were
-- wrong, this is where it would show up.
select
    order_id,
    line_no,
    quantity,
    unit_price,
    extended_amount
from {{ ref("order_item_current") }}
where round(extended_amount, 2) <> round(quantity * unit_price, 2)
