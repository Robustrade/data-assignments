import json
from .config import VALIDATION_FILE

def validate_warehouse(warehouse):
    con = warehouse.con
    failures = []

    checks = [
        ("customers_pk", "SELECT customer_id, COUNT(*) c FROM customers GROUP BY customer_id HAVING c > 1"),
        ("orders_customer_fk", "SELECT o.order_id FROM orders o LEFT JOIN customers c ON o.customer_id=c.customer_id WHERE c.customer_id IS NULL"),
        ("order_items_order_fk", "SELECT i.order_id FROM order_items i LEFT JOIN orders o ON i.order_id=o.order_id WHERE o.order_id IS NULL"),
        ("order_items_product_fk", "SELECT i.product_id FROM order_items i LEFT JOIN products p ON i.product_id=p.product_id WHERE p.product_id IS NULL"),
        ("orders_non_negative", "SELECT order_id FROM orders WHERE order_total < 0"),
        ("payments_non_negative", "SELECT payment_id FROM payments WHERE amount < 0"),
        ("status_domain", """SELECT order_id FROM orders WHERE status NOT IN ('PENDING','PAID','SHIPPED','CANCELLED')"""),
        ("order_item_totals", """
            SELECT o.order_id
            FROM orders o
            LEFT JOIN (
              SELECT order_id, ROUND(SUM(quantity * unit_price), 2) total
              FROM order_items GROUP BY order_id
            ) i ON o.order_id=i.order_id
            WHERE i.total IS NULL OR ROUND(o.order_total,2) != i.total
        """),
    ]
    for name, sql in checks:
        count = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        if count:
            failures.append({"check": name, "failures": count})
    VALIDATION_FILE.parent.mkdir(parents=True, exist_ok=True)
    with VALIDATION_FILE.open("w") as f:
        for item in failures:
            f.write(json.dumps(item) + "\n")
    if failures:
        raise AssertionError(f"Warehouse validation failed: {failures}")
    return True
