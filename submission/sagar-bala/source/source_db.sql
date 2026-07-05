-- CDC Lakehouse Reliability Assignment: Source Schema & Seed Data
-- Transactional Source Database: E-Commerce Orders and Line Items

-- =====================================================================
-- 1. DDL SCHEMA DEFINITION
-- =====================================================================

-- CUSTOMERS (Strong Entity)
-- Represents registered customer accounts
CREATE TABLE customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DEACTIVATED')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_updated_at ON customers(updated_at);


-- ORDERS (Strong Entity)
-- Represents customer orders. Lifecycle shifts from PENDING -> PAID -> SHIPPED (or CANCELLED)
CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL REFERENCES customers(customer_id),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')),
    total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0.00 CHECK (total_amount >= 0.00),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    shipping_address TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);


-- ORDER_ITEMS (Weak Entity - dependent on orders)
-- Represents individual products purchased within an order
CREATE TABLE order_items (
    order_item_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0.00),
    item_subtotal DECIMAL(12, 2) NOT NULL CHECK (item_subtotal >= 0.00),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_order_items_order_product ON order_items(order_id, product_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_updated_at ON order_items(updated_at);


-- PAYMENT_ATTEMPTS (Weak Entity - dependent on orders)
-- Tracks transaction attempts for orders
CREATE TABLE payment_attempts (
    payment_attempt_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(order_id),
    amount DECIMAL(12, 2) NOT NULL CHECK (amount >= 0.00),
    gateway VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'SUCCESSFUL', 'FAILED')),
    error_code VARCHAR(100), -- populated only if status is FAILED
    attempted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payment_attempts_order_id ON payment_attempts(order_id);
CREATE INDEX idx_payment_attempts_status ON payment_attempts(status);
CREATE INDEX idx_payment_attempts_attempted_at ON payment_attempts(attempted_at);


-- =====================================================================
-- 2. SEED DATA RECORDS
-- =====================================================================

-- Insert Customers
INSERT INTO customers (customer_id, email, first_name, last_name, status, created_at, updated_at) VALUES
('cust_001', 'alice.smith@example.com', 'Alice', 'Smith', 'ACTIVE', '2026-07-06T00:00:00Z', '2026-07-06T00:00:00Z'),
('cust_002', 'bob.jones@example.com', 'Bob', 'Jones', 'ACTIVE', '2026-07-06T00:05:00Z', '2026-07-06T00:05:00Z'),
('cust_003', 'charlie.brown@example.com', 'Charlie', 'Brown', 'SUSPENDED', '2026-07-06T00:10:00Z', '2026-07-06T00:12:00Z');

-- Insert Orders
INSERT INTO orders (order_id, customer_id, status, total_amount, currency, shipping_address, notes, created_at, updated_at, shipped_at) VALUES
('ord_101', 'cust_001', 'SHIPPED', 150.50, 'USD', '123 Main St, Seattle, WA', 'Leave at front door', '2026-07-06T00:15:00Z', '2026-07-06T01:30:00Z', '2026-07-06T01:30:00Z'),
('ord_102', 'cust_002', 'PAID', 45.00, 'USD', '456 Elm St, Boston, MA', NULL, '2026-07-06T00:20:00Z', '2026-07-06T00:22:00Z', NULL),
('ord_103', 'cust_001', 'PENDING', 99.99, 'USD', '123 Main St, Seattle, WA', 'Gift wrap please', '2026-07-06T00:30:00Z', '2026-07-06T00:30:00Z', NULL);

-- Insert Order Items
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, item_subtotal, created_at, updated_at) VALUES
('item_201', 'ord_101', 'prod_abc', 1, 100.00, 100.00, '2026-07-06T00:15:00Z', '2026-07-06T00:15:00Z'),
('item_202', 'ord_101', 'prod_xyz', 2, 25.25, 50.50, '2026-07-06T00:15:00Z', '2026-07-06T00:15:00Z'),
('item_203', 'ord_102', 'prod_def', 3, 15.00, 45.00, '2026-07-06T00:20:00Z', '2026-07-06T00:20:00Z'),
('item_204', 'ord_103', 'prod_abc', 1, 99.99, 99.99, '2026-07-06T00:30:00Z', '2026-07-06T00:30:00Z');

-- Insert Payment Attempts
INSERT INTO payment_attempts (payment_attempt_id, order_id, amount, gateway, status, error_code, attempted_at) VALUES
('pay_301', 'ord_101', 150.50, 'STRIPE', 'SUCCESSFUL', NULL, '2026-07-06T00:16:00Z'),
('pay_302', 'ord_102', 45.00, 'PAYPAL', 'SUCCESSFUL', NULL, '2026-07-06T00:22:00Z'),
('pay_303', 'ord_103', 99.99, 'STRIPE', 'FAILED', 'CARD_DECLINED', '2026-07-06T00:31:00Z');
