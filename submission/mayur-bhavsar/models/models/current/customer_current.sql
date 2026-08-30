-- Latest snapshot of customer: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_customer_history"), "customer") }}
