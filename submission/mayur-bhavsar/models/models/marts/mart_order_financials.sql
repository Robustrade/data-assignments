-- Order-level financial rollup. Exists to make the cross-entity business
-- validations (line-item totals, settlement coverage) expressible as tests.
{{
    mart_order_financials_sql(
        ref("customer_order_current"),
        ref("order_item_current"),
        ref("payment_current"),
        ref("customer_current"),
    )
}}
