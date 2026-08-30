{{ config(materialized="table") }}

-- Typed, deduplicated change log. Everything downstream reads this, never the
-- raw lake files, so deduplication and timestamp parsing happen exactly once.
{{ stg_cdc_events_sql(source("lake", "cdc_events")) }}
