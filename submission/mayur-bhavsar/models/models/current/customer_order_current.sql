-- Latest snapshot of customer_order: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_customer_order_history"), "customer_order") }}
