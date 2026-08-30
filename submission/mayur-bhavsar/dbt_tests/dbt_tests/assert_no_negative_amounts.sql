-- Mirrors the source CHECKs: no monetary column may be negative.
select 'customer_order.total_amount' as rule, cast(order_id as varchar) as key
from {{ ref("customer_order_current") }}
where total_amount < 0

union all

select 'order_item.extended_amount', cast(order_id as varchar) || ':' || cast(line_no as varchar)
from {{ ref("order_item_current") }}
where extended_amount < 0 or unit_price < 0 or quantity <= 0

union all

select 'payment.amount', cast(payment_id as varchar)
from {{ ref("payment_current") }}
where amount < 0

union all

select 'product.unit_price', cast(product_id as varchar)
from {{ ref("product_current") }}
where unit_price < 0
