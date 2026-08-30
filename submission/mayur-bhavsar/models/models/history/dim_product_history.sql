-- SCD2 history for product, derived only from the change log.
{{ scd2_from_cdc("product", ref("stg_cdc_events")) }}
