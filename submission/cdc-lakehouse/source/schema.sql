-- E-commerce source schema (MySQL 8)
-- Strong: customers, products, orders, payments
-- Weak: order_items, order_status_events

CREATE DATABASE IF NOT EXISTS cdc_source;
USE cdc_source;

-- ---------------------------------------------------------------------------
-- CDC outbox (simulates binlog / Debezium in production)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cdc_events (
    event_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(64)  NOT NULL,
    op           CHAR(1)      NOT NULL,
    pk_json      JSON         NOT NULL,
    row_json     JSON         NOT NULL,
    event_ts     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_cdc_events_event_id (event_id),
    INDEX idx_cdc_events_source_table (source_table)
);

-- ---------------------------------------------------------------------------
-- Business tables
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NULL,
    phone       VARCHAR(50)  NULL,
    status      ENUM('ACTIVE', 'SUSPENDED', 'CLOSED') NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    is_deleted  TINYINT(1)   NOT NULL DEFAULT 0,
    INDEX idx_customers_updated_at (updated_at),
    INDEX idx_customers_status (status)
);

CREATE TABLE IF NOT EXISTS products (
    product_id  BIGINT AUTO_INCREMENT PRIMARY KEY,
    sku         VARCHAR(64)  NOT NULL,
    name        VARCHAR(255) NOT NULL,
    unit_price  DECIMAL(12, 2) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    is_deleted  TINYINT(1)   NOT NULL DEFAULT 0,
    UNIQUE KEY uk_products_sku (sku),
    INDEX idx_products_updated_at (updated_at),
    CONSTRAINT chk_products_unit_price CHECK (unit_price >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    order_status  ENUM('CREATED', 'PAID', 'SHIPPED', 'CANCELLED', 'REFUNDED') NOT NULL DEFAULT 'CREATED',
    order_total   DECIMAL(12, 2) NOT NULL DEFAULT 0,
    created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    paid_at       TIMESTAMP(6) NULL,
    cancel_reason VARCHAR(512) NULL,
    is_deleted    TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_orders_customer_id (customer_id),
    INDEX idx_orders_updated_at (updated_at),
    INDEX idx_orders_status (order_status),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
    CONSTRAINT chk_orders_total CHECK (order_total >= 0)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id      BIGINT NOT NULL,
    product_id    BIGINT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    DECIMAL(12, 2) NOT NULL,
    line_total    DECIMAL(12, 2) NOT NULL,
    is_deleted    TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_order_items_order_id (order_id),
    INDEX idx_order_items_product_id (product_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders (order_id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products (product_id),
    CONSTRAINT chk_order_items_qty CHECK (quantity > 0),
    CONSTRAINT chk_order_items_prices CHECK (unit_price >= 0 AND line_total >= 0)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id       BIGINT NOT NULL,
    payment_amount DECIMAL(12, 2) NOT NULL,
    payment_status ENUM('PENDING', 'CAPTURED', 'FAILED', 'REFUNDED') NOT NULL DEFAULT 'PENDING',
    created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    is_deleted     TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_payments_order_id (order_id),
    INDEX idx_payments_updated_at (updated_at),
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders (order_id),
    CONSTRAINT chk_payments_amount CHECK (payment_amount >= 0)
);

CREATE TABLE IF NOT EXISTS order_status_events (
    status_event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    from_status     ENUM('CREATED', 'PAID', 'SHIPPED', 'CANCELLED', 'REFUNDED') NULL,
    to_status       ENUM('CREATED', 'PAID', 'SHIPPED', 'CANCELLED', 'REFUNDED') NOT NULL,
    event_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    is_deleted      TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_status_events_order_id (order_id),
    CONSTRAINT fk_status_events_order FOREIGN KEY (order_id) REFERENCES orders (order_id)
);
