-- Retention invariant: a delete must appear in history as a closed, flagged
-- version — never as a missing row. If a delete event exists in staging, the
-- matching history row must exist and be marked deleted.
{% for entity in cdc_entities() %}
select
    '{{ entity }}' as model,
    events.pk_hash,
    events.event_id
from {{ ref("stg_cdc_events") }} as events
left join {{ ref("dim_" ~ entity ~ "_history") }} as history
    on history.pk_hash = events.pk_hash
    and history._version = events.event_id
    and history.is_deleted
where events.table_name = '{{ entity }}'
  and events.op = 'D'
  and history.pk_hash is null
{% if not loop.last %}union all{% endif %}
{% endfor %}
