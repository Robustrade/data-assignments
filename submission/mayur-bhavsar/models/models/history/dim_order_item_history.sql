-- SCD2 history for order_item, derived only from the change log.
{{ scd2_from_cdc("order_item", ref("stg_cdc_events")) }}
