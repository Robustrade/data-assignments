{#-
    Warehouse SQL, held in macros rather than inline in the models.

    Two consumers need the same SQL:
      1. the dbt models, which pass ref() relations;
      2. the on-run-start bootstrap in macros/bootstrap.sql, which passes plain
         relation names so that `dbt test` is runnable in CI without a prior
         `dbt run` (see docs/07-ci-and-testing.md).

    Keeping the SQL in one place means those two paths cannot drift.
-#}


{#- Typed, deduplicated view of the raw change log. ------------------------- #}
{% macro stg_cdc_events_sql(relation) %}
with raw as (

    select * from {{ relation }}

),

ranked as (

    select
        *,
        -- At-least-once delivery means the same source event can land in the
        -- lake more than once (crash between the data write and the manifest
        -- append). (table_name, event_id) is the merge key that absorbs it.
        row_number() over (
            partition by table_name, event_id
            order by batch_id, ingested_at
        ) as _duplicate_rank
    from raw

)

select
    event_id,
    table_name,
    table_name || ':' || cast(event_id as varchar) as event_key,
    op,
    pk,
    pk_hash,
    before,
    after,
    -- strptime rather than a cast: a timestamp that does not match the
    -- published contract format should fail loudly, not be coerced.
    strptime(committed_at, '%Y-%m-%dT%H:%M:%S.%fZ') as committed_at,
    strptime(ingested_at, '%Y-%m-%dT%H:%M:%S.%fZ') as ingested_at,
    batch_id,
    contract_version
from ranked
where _duplicate_rank = 1
{% endmacro %}


{#- SCD2 history for one entity, derived purely from the change log. -------- #}
{% macro scd2_from_cdc(entity, relation) %}
{%- set spec = cdc_entities()[entity] -%}
with events as (

    select * from {{ relation }} where table_name = '{{ entity }}'

),

typed as (

    select
        pk_hash,
        pk as primary_key_json,
        event_id as _version,
        op as _op,
        committed_at,
        ingested_at,
        batch_id,
        -- A delete carries only a before image, so the last known attribute
        -- values come from coalesce(after, before). Without this the deleted
        -- row would be an all-NULL tombstone and point-in-time reconstruction
        -- of the moment before the delete would be impossible.
        {%- for col in spec.columns %}
        cast(
            json_extract_string(coalesce(after, before), '$.{{ col.name }}')
            as {{ col.type }}
        ) as {{ col.name }}{{ "," if not loop.last }}
        {%- endfor %}
    from events

),

windowed as (

    select
        *,
        -- Ordering is by _version (the source event id), never by arrival
        -- time and never by wall clock. This is what makes the warehouse a
        -- pure function of the lake: replaying the same events in any order
        -- produces the same table.
        lead(committed_at) over w as _next_committed_at,
        lead(_version) over w as _next_version
    from typed
    window w as (partition by pk_hash order by _version)

)

select
    pk_hash,
    pk_hash || ':' || cast(_version as varchar) as scd_key,
    primary_key_json,
    {%- for col in spec.columns %}
    {{ col.name }},
    {%- endfor %}
    _op,
    _version,
    committed_at as valid_from,
    _next_committed_at as valid_to,
    _next_version is null as is_current,
    _op = 'D' as is_deleted,
    ingested_at as _ingested_at,
    batch_id as _batch_id
from windowed
{% endmacro %}


{#- Latest snapshot: the open interval, minus tombstones. ------------------- #}
{% macro current_from_history(history_relation, entity) %}
{%- set spec = cdc_entities()[entity] -%}
select
    pk_hash,
    {%- for col in spec.columns %}
    {{ col.name }},
    {%- endfor %}
    _version,
    valid_from
from {{ history_relation }}
where is_current
  and not is_deleted
{% endmacro %}


{#-
    Point-in-time reconstruction is deliberately NOT a macro here.

    It takes a runtime parameter, so it cannot be a model, and a second copy of
    the rule in Jinja would be free to drift from the one operators actually
    run. There is exactly one implementation, in cdclake/warehouse.py
    (`AS_OF_SQL`), and docs/runbook.md quotes it verbatim.
-#}


{#- Analytical mart. -------------------------------------------------------- #}
{% macro mart_order_financials_sql(order_rel, item_rel, payment_rel, customer_rel) %}
with orders as (

    select * from {{ order_rel }}

),

items as (

    select
        order_id,
        sum(extended_amount) as line_item_total,
        count(*) as line_count
    from {{ item_rel }}
    group by 1

),

settled as (

    select
        order_id,
        sum(amount) as settled_amount
    from {{ payment_rel }}
    where status = 'SETTLED'
    group by 1

),

customers as (

    select
        customer_id,
        email,
        status as customer_status
    from {{ customer_rel }}

)

select
    o.order_id,
    o.customer_id,
    c.email,
    c.customer_status,
    o.status as order_status,
    o.currency,
    o.total_amount,
    o.placed_at,
    o.cancelled_at,
    coalesce(i.line_item_total, cast(0 as decimal(18, 2))) as line_item_total,
    coalesce(i.line_count, 0) as line_count,
    coalesce(s.settled_amount, cast(0 as decimal(18, 2))) as settled_amount,
    o.total_amount
    - coalesce(s.settled_amount, cast(0 as decimal(18, 2))) as outstanding_amount
from orders as o
left join customers as c on c.customer_id = o.customer_id
left join items as i on i.order_id = o.order_id
left join settled as s on s.order_id = o.order_id
{% endmacro %}
