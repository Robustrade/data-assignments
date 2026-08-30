-- Business rule: an order header total must equal the sum of its line items.
--
-- The source cannot enforce this with a CHECK (it spans rows), so it lives in
-- the application there. That is precisely why the warehouse has to re-assert
-- it: a source-side rule with no database constraint behind it is exactly the
-- rule most likely to be violated.
select
    order_id,
    total_amount,
    line_item_total,
    total_amount - line_item_total as difference
from {{ ref("mart_order_financials") }}
where line_count > 0
  and total_amount <> line_item_total
