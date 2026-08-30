-- SCD2 history for payment, derived only from the change log.
{{ scd2_from_cdc("payment", ref("stg_cdc_events")) }}
