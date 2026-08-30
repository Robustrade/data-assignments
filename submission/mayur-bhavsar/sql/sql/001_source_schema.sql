-- ============================================================================
-- Source system DDL — a small e-commerce order-to-cash domain.
--
-- Conventions
--   * Timestamps are TEXT in strict UTC ISO-8601 ('YYYY-MM-DDTHH:MM:SS.sssZ').
--     SQLite has no timestamp type; the format is lexicographically sortable,
--     which is what the CDC reader and the warehouse rely on.
--   * Money is declared DECIMAL(12,2). SQLite applies NUMERIC affinity, so the
--     value is physically stored as a float; every monetary CHECK is therefore
--     written with ROUND(x, 2) and the warehouse casts to DECIMAL(18,2) on
--     ingest so downstream arithmetic is exact. See docs/adr/0002.
--   * Enum-like fields are TEXT + a CHECK ... IN (...) domain. The schema gate
--     parses these domains out of sqlite_master and treats removing a member
--     as a breaking change. Postgres would use a native ENUM type.
-- ============================================================================

-- ── customer ────────────────────────────────────────────────────────────────
-- STRONG entity. Exists independently; referenced by orders.
CREATE TABLE IF NOT EXISTS customer (
    customer_id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    status TEXT NOT NULL
    CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    phone TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- ── product ─────────────────────────────────────────────────────────────────
-- STRONG entity. Slowly changing; price edits are the interesting change
-- pattern because they must not retroactively alter historical order lines.
CREATE TABLE IF NOT EXISTS product (
    product_id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL
    CHECK (unit_price >= 0 AND ROUND(unit_price, 2) = unit_price),
    currency TEXT NOT NULL CHECK (LENGTH(currency) = 3),
    discontinued_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- ── customer_order ──────────────────────────────────────────────────────────
-- STRONG entity. Named customer_order because ORDER is reserved.
-- High-churn: status walks PENDING -> PAID -> SHIPPED -> DELIVERED.
CREATE TABLE IF NOT EXISTS customer_order (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customer (customer_id),
    status TEXT NOT NULL
    CHECK (
        status IN ('PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED')
    ),
    currency TEXT NOT NULL CHECK (LENGTH(currency) = 3),
    total_amount DECIMAL(12, 2) NOT NULL
    CHECK (total_amount >= 0 AND ROUND(total_amount, 2) = total_amount),
    placed_at TEXT NOT NULL,
    cancelled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- A cancellation timestamp may only exist on a cancelled order, and never
    -- before the order was placed.
    CHECK (cancelled_at IS NULL OR status = 'CANCELLED'),
    CHECK (cancelled_at IS NULL OR cancelled_at >= placed_at)
);

-- ── order_item ──────────────────────────────────────────────────────────────
-- WEAK entity: line_no is only unique within an order, and a line cannot
-- exist without its order. Identifying relationship => composite PK and
-- ON DELETE CASCADE.
CREATE TABLE IF NOT EXISTS order_item (
    order_id INTEGER NOT NULL
    REFERENCES customer_order (order_id) ON DELETE CASCADE,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    product_id INTEGER NOT NULL REFERENCES product (product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(12, 2) NOT NULL
    CHECK (unit_price >= 0 AND ROUND(unit_price, 2) = unit_price),
    extended_amount DECIMAL(12, 2) NOT NULL
    CHECK (ROUND(extended_amount, 2) = ROUND(quantity * unit_price, 2)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- ── payment ─────────────────────────────────────────────────────────────────
-- STRONG entity (own surrogate key) with a mandatory parent order.
CREATE TABLE IF NOT EXISTS payment (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES customer_order (order_id),
    method TEXT NOT NULL
    CHECK (method IN ('CARD', 'BANK_TRANSFER', 'WALLET')),
    amount DECIMAL(12, 2) NOT NULL
    CHECK (amount >= 0 AND ROUND(amount, 2) = amount),
    currency TEXT NOT NULL CHECK (LENGTH(currency) = 3),
    status TEXT NOT NULL
    CHECK (status IN ('INITIATED', 'SETTLED', 'FAILED', 'REFUNDED')),
    created_at TEXT NOT NULL,
    settled_at TEXT,
    updated_at TEXT NOT NULL,
    -- Settlement can never precede creation.
    CHECK (settled_at IS NULL OR settled_at >= created_at),
    -- Only a settled or refunded payment carries a settlement timestamp.
    CHECK (settled_at IS NULL OR status IN ('SETTLED', 'REFUNDED'))
);

-- ── payment_attempt ─────────────────────────────────────────────────────────
-- WEAK entity: attempt_no is only meaningful within a payment. This is the
-- highest-churn table in the domain (gateway retries) and therefore the one
-- that makes CDC volume realistic.
CREATE TABLE IF NOT EXISTS payment_attempt (
    payment_id INTEGER NOT NULL
    REFERENCES payment (payment_id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    result TEXT NOT NULL
    CHECK (result IN ('SUCCESS', 'DECLINED', 'ERROR', 'TIMEOUT')),
    failure_code TEXT,
    attempted_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- A successful attempt has no failure code; a failed one must have one.
    CHECK (
        (result = 'SUCCESS' AND failure_code IS NULL)
        OR (result <> 'SUCCESS' AND failure_code IS NOT NULL)
    ),
    PRIMARY KEY (payment_id, attempt_no)
);

-- ── indexes ─────────────────────────────────────────────────────────────────
-- Each index exists for a stated access or change-capture pattern.

-- Order history for a customer: the single most common transactional lookup.
CREATE INDEX IF NOT EXISTS idx_customer_order_customer_placed
ON customer_order (customer_id, placed_at);

-- Supports the timestamp-watermark CDC fallback described in docs/02, and
-- operational "what changed recently" queries.
CREATE INDEX IF NOT EXISTS idx_customer_order_updated
ON customer_order (updated_at);

-- Referential-integrity assertions and product-level revenue rollups both
-- probe order_item by product.
CREATE INDEX IF NOT EXISTS idx_order_item_product
ON order_item (product_id);

-- Payments are almost always fetched by their parent order.
CREATE INDEX IF NOT EXISTS idx_payment_order
ON payment (order_id);

-- Settlement monitoring: "unsettled payments older than N minutes".
CREATE INDEX IF NOT EXISTS idx_payment_status_created
ON payment (status, created_at);

-- Retry analysis on the highest-churn table.
CREATE INDEX IF NOT EXISTS idx_payment_attempt_result
ON payment_attempt (result, attempted_at);
