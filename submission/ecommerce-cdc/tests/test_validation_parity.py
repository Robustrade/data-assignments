"""Validation parity tests: system + business rules mirrored in the warehouse."""

from validation.checks import run_all_checks, run_business_checks, run_system_checks


def test_clean_warehouse_passes_all_checks(ingested):
    assert run_all_checks(ingested) == []


def test_negative_payment_amount_is_caught(ingested):
    ingested.execute("UPDATE wh_payments SET amount = -5 WHERE payment_id = 'pay1'")
    failures = run_business_checks(ingested)
    assert any("negative amount" in f for f in failures)


def test_order_total_mismatch_is_caught(ingested):
    ingested.execute("UPDATE wh_orders SET total = 999 WHERE order_id = 'o1'")
    failures = run_business_checks(ingested)
    assert any("sum(order_items.line_total)" in f for f in failures)


def test_line_total_mismatch_is_caught(ingested):
    ingested.execute(
        "UPDATE wh_order_items SET line_total = 1 WHERE order_item_id = 'oi1'"
    )
    failures = run_business_checks(ingested)
    assert any("line_total" in f for f in failures)


def test_referential_integrity_violation_is_caught(ingested):
    ingested.execute("UPDATE wh_orders SET customer_id = 'ghost' WHERE order_id = 'o1'")
    failures = run_system_checks(ingested)
    assert any("orphan" in f for f in failures)


def test_enum_domain_violation_is_caught(ingested):
    ingested.execute("UPDATE wh_orders SET status = 'invalid' WHERE order_id = 'o1'")
    failures = run_system_checks(ingested)
    assert any("enum domain" in f for f in failures)


def test_illegal_status_transition_is_caught(conn):
    from ingestion.cdc import CDCCapture
    from ingestion.pipeline import run_ingestion
    from ingestion.seed import BASE_TS

    cap = CDCCapture()
    cap.insert(
        "customers",
        "c1",
        {
            "customer_id": "c1",
            "name": "A",
            "email": "a@x.com",
            "status": "active",
            "created_at": BASE_TS,
            "updated_at": BASE_TS,
        },
    )
    order = {
        "order_id": "o1",
        "customer_id": "c1",
        "status": "pending",
        "total": 0.00,
        "currency": "USD",
        "created_at": BASE_TS,
        "updated_at": BASE_TS,
    }
    cap.insert("orders", "o1", order)
    # Illegal: pending -> delivered (must go through paid/shipped).
    cap.update("orders", "o1", {**order, "status": "delivered"}, before=order)
    run_ingestion(conn, cap)

    failures = run_business_checks(conn)
    assert any("illegal status transition" in f for f in failures)
