-- SCD2 history for payment_attempt, derived only from the change log.
{{ scd2_from_cdc("payment_attempt", ref("stg_cdc_events")) }}
