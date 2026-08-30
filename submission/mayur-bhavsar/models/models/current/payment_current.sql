-- Latest snapshot of payment: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_payment_history"), "payment") }}
