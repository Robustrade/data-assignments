-- Business rule the source cannot express at all: which status transitions are
-- legal. A snapshot-only warehouse could never check this. Because the history
-- model keeps every version in order, the transition itself becomes a row.
with versions as (

    select
        pk_hash,
        order_id,
        _version,
        _op,
        status,
        lag(status) over (partition by pk_hash order by _version) as previous_status
    from {{ ref("dim_customer_order_history") }}

)

select
    order_id,
    _version,
    previous_status,
    status as new_status
from versions
where previous_status is not null
  and _op <> 'D'
  and status <> previous_status
  and not (
      (previous_status = 'PENDING' and status in ('PAID', 'CANCELLED'))
      or (previous_status = 'PAID' and status in ('SHIPPED', 'CANCELLED'))
      or (previous_status = 'SHIPPED' and status = 'DELIVERED')
  )
