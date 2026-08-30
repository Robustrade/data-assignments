{#-
    on-run-start bootstrap.

    Why this exists
    ---------------
    The repository's CI workflow runs `dbt parse` and then `dbt test`, with no
    `dbt run` in between and no pipeline execution beforehand. Left alone,
    `dbt test` would fail on missing relations and the honest fix — deleting
    dbt_project.yml so the job prints "skipping" — would make the check
    vacuously green.

    Instead the hook materialises the warehouse from a committed lake extract,
    using exactly the macros the models use. The check therefore exercises real
    model SQL and real assertions. See docs/07-ci-and-testing.md.

    The lake relation is always (re)built, because that is the ingest boundary.
    The models are only bootstrapped for `dbt test`; for `dbt run` and
    `dbt build` dbt materialises them itself moments later.
-#}

{% macro lake_glob() %}
    {{- return(env_var("CDCLAKE_LAKE_GLOB", "warehouse/fixtures/*.ndjson")) -}}
{% endmacro %}


{% macro build_lake_relation_sql() %}
create or replace table main.lake_cdc_events as
select *
from read_json(
    '{{ lake_glob() }}',
    format = 'newline_delimited',
    -- The lake schema is pinned, not inferred. It is deliberately independent
    -- of the source schema: pk / before / after are JSON text, so a source
    -- schema change produces different JSON, never a different file layout.
    columns = {
        'event_id': 'BIGINT',
        'table_name': 'VARCHAR',
        'op': 'VARCHAR',
        'pk': 'VARCHAR',
        'pk_hash': 'VARCHAR',
        'before': 'VARCHAR',
        'after': 'VARCHAR',
        'committed_at': 'VARCHAR',
        'ingested_at': 'VARCHAR',
        'batch_id': 'BIGINT',
        'contract_version': 'BIGINT'
    }
)
{% endmacro %}


{% macro bootstrap_models() %}
    {% do run_query(
        "create or replace table main.stg_cdc_events as "
        ~ stg_cdc_events_sql("main.lake_cdc_events")
    ) %}
    {% for entity in cdc_entities() %}
        {% do run_query(
            "create or replace table main.dim_" ~ entity ~ "_history as "
            ~ scd2_from_cdc(entity, "main.stg_cdc_events")
        ) %}
    {% endfor %}
    {% for entity in cdc_entities() %}
        {% do run_query(
            "create or replace view main." ~ entity ~ "_current as "
            ~ current_from_history("main.dim_" ~ entity ~ "_history", entity)
        ) %}
    {% endfor %}
    {% do run_query(
        "create or replace view main.mart_order_financials as "
        ~ mart_order_financials_sql(
            "main.customer_order_current",
            "main.order_item_current",
            "main.payment_current",
            "main.customer_current"
        )
    ) %}
{% endmacro %}


{% macro bootstrap_warehouse() %}
    {% if execute %}
        {% do run_query(build_lake_relation_sql()) %}
        {% if flags.WHICH == "test" %}
            {% do log("bootstrapping warehouse models for standalone dbt test", info=true) %}
            {% do bootstrap_models() %}
        {% endif %}
    {% endif %}
    select 1 as bootstrap_ok
{% endmacro %}
