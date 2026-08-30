-- Latest snapshot of order_item: the open SCD2 interval, tombstones excluded.
{{ current_from_history(ref("dim_order_item_history"), "order_item") }}
