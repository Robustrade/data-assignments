USE cdc_source;

INSERT INTO customers (name, email, status) VALUES
  ('Alice Example', 'alice@example.com', 'ACTIVE'),
  ('Bob Example', 'bob@example.com', 'ACTIVE');

INSERT INTO products (sku, name, unit_price) VALUES
  ('SKU-001', 'Widget A', 19.99),
  ('SKU-002', 'Widget B', 29.50);

INSERT INTO orders (customer_id, order_status, order_total) VALUES
  (1, 'CREATED', 39.98);

INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
  (1, 1, 2, 19.99, 39.98);

INSERT INTO order_status_events (order_id, from_status, to_status) VALUES
  (1, NULL, 'CREATED');

INSERT INTO payments (order_id, payment_amount, payment_status) VALUES
  (1, 39.98, 'PENDING');

UPDATE orders SET order_status = 'PAID', paid_at = CURRENT_TIMESTAMP(6) WHERE order_id = 1;
UPDATE payments SET payment_status = 'CAPTURED' WHERE payment_id = 1;

INSERT INTO order_status_events (order_id, from_status, to_status) VALUES
  (1, 'CREATED', 'PAID');
