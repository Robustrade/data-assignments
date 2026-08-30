-- ============================================================================
-- Change data capture, implemented inside the source database.
--
-- Every DML statement on a tracked table fires an AFTER trigger that appends
-- one row to cdc_events with the full before and after images. This is a
-- genuine database-level capture mechanism: it cannot be bypassed by an
-- application forgetting to emit an event, and it captures deletes, which a
-- timestamp-watermark extraction never can.
--
-- The triggers are written by hand rather than generated from contracts/*.json
-- on purpose: the source system is owned by another team. If the pipeline
-- generated its own source, the schema gate in docs/03 would be vacuous —
-- it could never detect drift because there would be nothing to drift from.
--
-- event_id is INTEGER PRIMARY KEY AUTOINCREMENT: strictly increasing, never
-- reused after a delete, and — because SQLite serialises writers — assigned in
-- commit order. That last property is what makes event_id a safe replay
-- offset here and is NOT true of a sequence-backed audit table on Postgres;
-- see docs/02 for why production uses the WAL LSN instead.
-- ============================================================================

CREATE TABLE IF NOT EXISTS cdc_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('I', 'U', 'D')),
    pk TEXT NOT NULL,
    before_image TEXT,
    after_image TEXT,
    committed_at TEXT NOT NULL,
    -- Invariant restated in the log itself: an insert has no before image,
    -- a delete has no after image, an update has both.
    CHECK (
        (op = 'I' AND before_image IS NULL AND after_image IS NOT NULL)
        OR (op = 'U' AND before_image IS NOT NULL AND after_image IS NOT NULL)
        OR (op = 'D' AND before_image IS NOT NULL AND after_image IS NULL)
    )
);

-- The consumer scans strictly forward from its checkpoint.
CREATE INDEX IF NOT EXISTS idx_cdc_events_table_event
ON cdc_events (table_name, event_id);


-- ── customer ────────────────────────────────────────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_customer_i AFTER INSERT ON customer
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer', 'I',
        json_object('customer_id', new.customer_id),
        NULL,
        json_object(
            'customer_id', new.customer_id, 'email', new.email,
            'full_name', new.full_name, 'status', new.status,
            'phone', new.phone, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_customer_u AFTER UPDATE ON customer
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer', 'U',
        json_object('customer_id', new.customer_id),
        json_object(
            'customer_id', old.customer_id, 'email', old.email,
            'full_name', old.full_name, 'status', old.status,
            'phone', old.phone, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        json_object(
            'customer_id', new.customer_id, 'email', new.email,
            'full_name', new.full_name, 'status', new.status,
            'phone', new.phone, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_customer_d AFTER DELETE ON customer
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer', 'D',
        json_object('customer_id', old.customer_id),
        json_object(
            'customer_id', old.customer_id, 'email', old.email,
            'full_name', old.full_name, 'status', old.status,
            'phone', old.phone, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;


-- ── product ─────────────────────────────────────────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_product_i AFTER INSERT ON product
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'product', 'I',
        json_object('product_id', new.product_id),
        NULL,
        json_object(
            'product_id', new.product_id, 'sku', new.sku, 'name', new.name,
            'unit_price', new.unit_price, 'currency', new.currency,
            'discontinued_at', new.discontinued_at,
            'created_at', new.created_at, 'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_product_u AFTER UPDATE ON product
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'product', 'U',
        json_object('product_id', new.product_id),
        json_object(
            'product_id', old.product_id, 'sku', old.sku, 'name', old.name,
            'unit_price', old.unit_price, 'currency', old.currency,
            'discontinued_at', old.discontinued_at,
            'created_at', old.created_at, 'updated_at', old.updated_at
        ),
        json_object(
            'product_id', new.product_id, 'sku', new.sku, 'name', new.name,
            'unit_price', new.unit_price, 'currency', new.currency,
            'discontinued_at', new.discontinued_at,
            'created_at', new.created_at, 'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_product_d AFTER DELETE ON product
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'product', 'D',
        json_object('product_id', old.product_id),
        json_object(
            'product_id', old.product_id, 'sku', old.sku, 'name', old.name,
            'unit_price', old.unit_price, 'currency', old.currency,
            'discontinued_at', old.discontinued_at,
            'created_at', old.created_at, 'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;


-- ── customer_order ──────────────────────────────────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_customer_order_i
AFTER INSERT ON customer_order
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer_order', 'I',
        json_object('order_id', new.order_id),
        NULL,
        json_object(
            'order_id', new.order_id, 'customer_id', new.customer_id,
            'status', new.status, 'currency', new.currency,
            'total_amount', new.total_amount, 'placed_at', new.placed_at,
            'cancelled_at', new.cancelled_at, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_customer_order_u
AFTER UPDATE ON customer_order
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer_order', 'U',
        json_object('order_id', new.order_id),
        json_object(
            'order_id', old.order_id, 'customer_id', old.customer_id,
            'status', old.status, 'currency', old.currency,
            'total_amount', old.total_amount, 'placed_at', old.placed_at,
            'cancelled_at', old.cancelled_at, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        json_object(
            'order_id', new.order_id, 'customer_id', new.customer_id,
            'status', new.status, 'currency', new.currency,
            'total_amount', new.total_amount, 'placed_at', new.placed_at,
            'cancelled_at', new.cancelled_at, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_customer_order_d
AFTER DELETE ON customer_order
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'customer_order', 'D',
        json_object('order_id', old.order_id),
        json_object(
            'order_id', old.order_id, 'customer_id', old.customer_id,
            'status', old.status, 'currency', old.currency,
            'total_amount', old.total_amount, 'placed_at', old.placed_at,
            'cancelled_at', old.cancelled_at, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;


-- ── order_item (weak entity, composite key) ─────────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_order_item_i AFTER INSERT ON order_item
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'order_item', 'I',
        json_object('order_id', new.order_id, 'line_no', new.line_no),
        NULL,
        json_object(
            'order_id', new.order_id, 'line_no', new.line_no,
            'product_id', new.product_id, 'quantity', new.quantity,
            'unit_price', new.unit_price,
            'extended_amount', new.extended_amount,
            'created_at', new.created_at, 'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_order_item_u AFTER UPDATE ON order_item
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'order_item', 'U',
        json_object('order_id', new.order_id, 'line_no', new.line_no),
        json_object(
            'order_id', old.order_id, 'line_no', old.line_no,
            'product_id', old.product_id, 'quantity', old.quantity,
            'unit_price', old.unit_price,
            'extended_amount', old.extended_amount,
            'created_at', old.created_at, 'updated_at', old.updated_at
        ),
        json_object(
            'order_id', new.order_id, 'line_no', new.line_no,
            'product_id', new.product_id, 'quantity', new.quantity,
            'unit_price', new.unit_price,
            'extended_amount', new.extended_amount,
            'created_at', new.created_at, 'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_order_item_d AFTER DELETE ON order_item
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'order_item', 'D',
        json_object('order_id', old.order_id, 'line_no', old.line_no),
        json_object(
            'order_id', old.order_id, 'line_no', old.line_no,
            'product_id', old.product_id, 'quantity', old.quantity,
            'unit_price', old.unit_price,
            'extended_amount', old.extended_amount,
            'created_at', old.created_at, 'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;


-- ── payment ─────────────────────────────────────────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_payment_i AFTER INSERT ON payment
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment', 'I',
        json_object('payment_id', new.payment_id),
        NULL,
        json_object(
            'payment_id', new.payment_id, 'order_id', new.order_id,
            'method', new.method, 'amount', new.amount,
            'currency', new.currency, 'status', new.status,
            'created_at', new.created_at, 'settled_at', new.settled_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_payment_u AFTER UPDATE ON payment
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment', 'U',
        json_object('payment_id', new.payment_id),
        json_object(
            'payment_id', old.payment_id, 'order_id', old.order_id,
            'method', old.method, 'amount', old.amount,
            'currency', old.currency, 'status', old.status,
            'created_at', old.created_at, 'settled_at', old.settled_at,
            'updated_at', old.updated_at
        ),
        json_object(
            'payment_id', new.payment_id, 'order_id', new.order_id,
            'method', new.method, 'amount', new.amount,
            'currency', new.currency, 'status', new.status,
            'created_at', new.created_at, 'settled_at', new.settled_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_payment_d AFTER DELETE ON payment
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment', 'D',
        json_object('payment_id', old.payment_id),
        json_object(
            'payment_id', old.payment_id, 'order_id', old.order_id,
            'method', old.method, 'amount', old.amount,
            'currency', old.currency, 'status', old.status,
            'created_at', old.created_at, 'settled_at', old.settled_at,
            'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;


-- ── payment_attempt (weak entity, composite key) ────────────────────────────
CREATE TRIGGER IF NOT EXISTS cdc_payment_attempt_i
AFTER INSERT ON payment_attempt
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment_attempt', 'I',
        json_object('payment_id', new.payment_id, 'attempt_no', new.attempt_no),
        NULL,
        json_object(
            'payment_id', new.payment_id, 'attempt_no', new.attempt_no,
            'result', new.result, 'failure_code', new.failure_code,
            'attempted_at', new.attempted_at, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_payment_attempt_u
AFTER UPDATE ON payment_attempt
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment_attempt', 'U',
        json_object('payment_id', new.payment_id, 'attempt_no', new.attempt_no),
        json_object(
            'payment_id', old.payment_id, 'attempt_no', old.attempt_no,
            'result', old.result, 'failure_code', old.failure_code,
            'attempted_at', old.attempted_at, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        json_object(
            'payment_id', new.payment_id, 'attempt_no', new.attempt_no,
            'result', new.result, 'failure_code', new.failure_code,
            'attempted_at', new.attempted_at, 'created_at', new.created_at,
            'updated_at', new.updated_at
        ),
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;

CREATE TRIGGER IF NOT EXISTS cdc_payment_attempt_d
AFTER DELETE ON payment_attempt
BEGIN
    INSERT INTO cdc_events (
        table_name, op, pk, before_image, after_image, committed_at
    )
    VALUES (
        'payment_attempt', 'D',
        json_object('payment_id', old.payment_id, 'attempt_no', old.attempt_no),
        json_object(
            'payment_id', old.payment_id, 'attempt_no', old.attempt_no,
            'result', old.result, 'failure_code', old.failure_code,
            'attempted_at', old.attempted_at, 'created_at', old.created_at,
            'updated_at', old.updated_at
        ),
        NULL,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    );
END;
