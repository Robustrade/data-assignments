-- SCD2 history for customer_order, derived only from the change log.
{{ scd2_from_cdc("customer_order", ref("stg_cdc_events")) }}
