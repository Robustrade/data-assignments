-- Completeness: deduplication must absorb duplicates without ever dropping a
-- distinct change. Every distinct event in the lake has to survive into
-- staging, or history is incomplete and every model above it is wrong.
with lake_events as (

    select distinct
        table_name || ':' || cast(event_id as varchar) as event_key
    from {{ source("lake", "cdc_events") }}

),

staged as (

    select event_key from {{ ref("stg_cdc_events") }}

)

select lake_events.event_key
from lake_events
left join staged on staged.event_key = lake_events.event_key
where staged.event_key is null
