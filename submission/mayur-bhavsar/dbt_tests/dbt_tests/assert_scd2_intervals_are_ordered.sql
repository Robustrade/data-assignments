-- SCD2 invariant: a closed interval never ends before it starts, and a closed
-- interval is never marked current.
{% for entity in cdc_entities() %}
select
    '{{ entity }}' as model,
    scd_key,
    valid_from,
    valid_to,
    is_current
from {{ ref("dim_" ~ entity ~ "_history") }}
where (valid_to is not null and valid_to < valid_from)
   or (valid_to is not null and is_current)
   or (valid_to is null and not is_current)
{% if not loop.last %}union all{% endif %}
{% endfor %}
