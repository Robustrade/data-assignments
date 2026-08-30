-- Latest snapshot of product: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_product_history"), "product") }}
