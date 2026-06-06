-- Canonical PostgreSQL source schema for the e-commerce CDC pipeline.
-- The runnable pipeline mirrors this in DuckDB (see source/models.py); in
-- production this is the system of record and CDC is read from its WAL.

CREATE TYPE customer_status AS ENUM ('active', 'suspended', 'closed');
CREATE TYPE product_status  AS ENUM ('active', 'discontinued');
CREATE TYPE order_status    AS ENUM ('pending', 'paid', 'shipped',
                                     'delivered', 'cancelled', 'refunded');
CREATE TYPE payment_method  AS ENUM ('card', 'wallet', 'bank_transfer');
CREATE TYPE payment_status  AS ENUM ('pending', 'settled', 'failed', 'refunded');

-- Strong entity: customers
CREATE TABLE customers (
    customer_id  TEXT PRIMARY KEY,
    name         TEXT            NOT NULL,
    email        TEXT            NOT NULL,
    status       customer_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Strong entity: products
CREATE TABLE products (
    product_id   TEXT PRIMARY KEY,
    sku          TEXT           NOT NULL UNIQUE,
    name         TEXT           NOT NULL,
    price        NUMERIC(18, 2) NOT NULL CHECK (price > 0),
    status       product_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Strong entity: orders
CREATE TABLE orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT           NOT NULL REFERENCES customers(customer_id),
    status       order_status   NOT NULL DEFAULT 'pending',
    total        NUMERIC(18, 2) NOT NULL CHECK (total >= 0),
    currency     TEXT           NOT NULL,
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Weak entity: order_items (identity completed by parent order)
CREATE TABLE order_items (
    order_item_id  TEXT PRIMARY KEY,
    order_id       TEXT           NOT NULL REFERENCES orders(order_id),
    product_id     TEXT           NOT NULL REFERENCES products(product_id),
    quantity       INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(18, 2) NOT NULL CHECK (unit_price >= 0),
    line_total     NUMERIC(18, 2) NOT NULL CHECK (line_total >= 0)
);

-- Weak entity: payments (lifecycle bound to parent order)
CREATE TABLE payments (
    payment_id   TEXT PRIMARY KEY,
    order_id     TEXT           NOT NULL REFERENCES orders(order_id),
    amount       NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    method       payment_method NOT NULL,
    status       payment_status NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    settled_at   TIMESTAMPTZ,
    CHECK (settled_at IS NULL OR settled_at >= created_at)
);

-- Indexes on FK and change-capture (updated_at) columns.
CREATE INDEX idx_orders_customer   ON orders(customer_id);
CREATE INDEX idx_orders_updated    ON orders(updated_at);
CREATE INDEX idx_items_order       ON order_items(order_id);
CREATE INDEX idx_items_product     ON order_items(product_id);
CREATE INDEX idx_payments_order    ON payments(order_id);
CREATE INDEX idx_customers_updated ON customers(updated_at);
