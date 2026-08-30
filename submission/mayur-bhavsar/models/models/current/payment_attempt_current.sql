-- Latest snapshot of payment_attempt: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_payment_attempt_history"), "payment_attempt") }}
