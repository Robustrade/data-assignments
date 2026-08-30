-- SCD2 history for customer, derived only from the change log.
{{ scd2_from_cdc("customer", ref("stg_cdc_events")) }}
