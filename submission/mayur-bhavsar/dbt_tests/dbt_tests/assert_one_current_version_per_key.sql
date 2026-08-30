-- SCD2 invariant: exactly one open interval per key, in every history model.
{% for entity in cdc_entities() %}
select
    '{{ entity }}' as model,
    pk_hash,
    count(*) as open_intervals
from {{ ref("dim_" ~ entity ~ "_history") }}
where is_current
group by 1, 2
having count(*) > 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
