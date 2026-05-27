USE cdc_source;

DELIMITER $$

-- customers
CREATE TRIGGER trg_customers_ai AFTER INSERT ON customers FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'customers', 'I',
    JSON_OBJECT('customer_id', NEW.customer_id),
    JSON_OBJECT('customer_id', NEW.customer_id, 'name', NEW.name, 'email', NEW.email,
      'phone', NEW.phone, 'status', NEW.status, 'created_at', NEW.created_at,
      'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_customers_au AFTER UPDATE ON customers FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'customers', 'U',
    JSON_OBJECT('customer_id', NEW.customer_id),
    JSON_OBJECT('customer_id', NEW.customer_id, 'name', NEW.name, 'email', NEW.email,
      'phone', NEW.phone, 'status', NEW.status, 'created_at', NEW.created_at,
      'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_customers_ad AFTER DELETE ON customers FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'customers', 'D',
    JSON_OBJECT('customer_id', OLD.customer_id),
    JSON_OBJECT('customer_id', OLD.customer_id, 'name', OLD.name, 'email', OLD.email,
      'phone', OLD.phone, 'status', OLD.status, 'created_at', OLD.created_at,
      'updated_at', OLD.updated_at, 'is_deleted', OLD.is_deleted)
  );
END$$

-- products
CREATE TRIGGER trg_products_ai AFTER INSERT ON products FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'products', 'I',
    JSON_OBJECT('product_id', NEW.product_id),
    JSON_OBJECT('product_id', NEW.product_id, 'sku', NEW.sku, 'name', NEW.name,
      'unit_price', NEW.unit_price, 'created_at', NEW.created_at,
      'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_products_au AFTER UPDATE ON products FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'products', 'U',
    JSON_OBJECT('product_id', NEW.product_id),
    JSON_OBJECT('product_id', NEW.product_id, 'sku', NEW.sku, 'name', NEW.name,
      'unit_price', NEW.unit_price, 'created_at', NEW.created_at,
      'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_products_ad AFTER DELETE ON products FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'products', 'D',
    JSON_OBJECT('product_id', OLD.product_id),
    JSON_OBJECT('product_id', OLD.product_id, 'sku', OLD.sku, 'name', OLD.name,
      'unit_price', OLD.unit_price, 'created_at', OLD.created_at,
      'updated_at', OLD.updated_at, 'is_deleted', OLD.is_deleted)
  );
END$$

-- orders
CREATE TRIGGER trg_orders_ai AFTER INSERT ON orders FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'orders', 'I',
    JSON_OBJECT('order_id', NEW.order_id),
    JSON_OBJECT('order_id', NEW.order_id, 'customer_id', NEW.customer_id,
      'order_status', NEW.order_status, 'order_total', NEW.order_total,
      'created_at', NEW.created_at, 'updated_at', NEW.updated_at,
      'paid_at', NEW.paid_at, 'cancel_reason', NEW.cancel_reason, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_orders_au AFTER UPDATE ON orders FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'orders', 'U',
    JSON_OBJECT('order_id', NEW.order_id),
    JSON_OBJECT('order_id', NEW.order_id, 'customer_id', NEW.customer_id,
      'order_status', NEW.order_status, 'order_total', NEW.order_total,
      'created_at', NEW.created_at, 'updated_at', NEW.updated_at,
      'paid_at', NEW.paid_at, 'cancel_reason', NEW.cancel_reason, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_orders_ad AFTER DELETE ON orders FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'orders', 'D',
    JSON_OBJECT('order_id', OLD.order_id),
    JSON_OBJECT('order_id', OLD.order_id, 'customer_id', OLD.customer_id,
      'order_status', OLD.order_status, 'order_total', OLD.order_total,
      'created_at', OLD.created_at, 'updated_at', OLD.updated_at,
      'paid_at', OLD.paid_at, 'cancel_reason', OLD.cancel_reason, 'is_deleted', OLD.is_deleted)
  );
END$$

-- order_items
CREATE TRIGGER trg_order_items_ai AFTER INSERT ON order_items FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_items', 'I',
    JSON_OBJECT('order_item_id', NEW.order_item_id),
    JSON_OBJECT('order_item_id', NEW.order_item_id, 'order_id', NEW.order_id,
      'product_id', NEW.product_id, 'quantity', NEW.quantity, 'unit_price', NEW.unit_price,
      'line_total', NEW.line_total, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_order_items_au AFTER UPDATE ON order_items FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_items', 'U',
    JSON_OBJECT('order_item_id', NEW.order_item_id),
    JSON_OBJECT('order_item_id', NEW.order_item_id, 'order_id', NEW.order_id,
      'product_id', NEW.product_id, 'quantity', NEW.quantity, 'unit_price', NEW.unit_price,
      'line_total', NEW.line_total, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_order_items_ad AFTER DELETE ON order_items FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_items', 'D',
    JSON_OBJECT('order_item_id', OLD.order_item_id),
    JSON_OBJECT('order_item_id', OLD.order_item_id, 'order_id', OLD.order_id,
      'product_id', OLD.product_id, 'quantity', OLD.quantity, 'unit_price', OLD.unit_price,
      'line_total', OLD.line_total, 'is_deleted', OLD.is_deleted)
  );
END$$

-- payments
CREATE TRIGGER trg_payments_ai AFTER INSERT ON payments FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'payments', 'I',
    JSON_OBJECT('payment_id', NEW.payment_id),
    JSON_OBJECT('payment_id', NEW.payment_id, 'order_id', NEW.order_id,
      'payment_amount', NEW.payment_amount, 'payment_status', NEW.payment_status,
      'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_payments_au AFTER UPDATE ON payments FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'payments', 'U',
    JSON_OBJECT('payment_id', NEW.payment_id),
    JSON_OBJECT('payment_id', NEW.payment_id, 'order_id', NEW.order_id,
      'payment_amount', NEW.payment_amount, 'payment_status', NEW.payment_status,
      'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_payments_ad AFTER DELETE ON payments FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'payments', 'D',
    JSON_OBJECT('payment_id', OLD.payment_id),
    JSON_OBJECT('payment_id', OLD.payment_id, 'order_id', OLD.order_id,
      'payment_amount', OLD.payment_amount, 'payment_status', OLD.payment_status,
      'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'is_deleted', OLD.is_deleted)
  );
END$$

-- order_status_events
CREATE TRIGGER trg_status_events_ai AFTER INSERT ON order_status_events FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_status_events', 'I',
    JSON_OBJECT('status_event_id', NEW.status_event_id),
    JSON_OBJECT('status_event_id', NEW.status_event_id, 'order_id', NEW.order_id,
      'from_status', NEW.from_status, 'to_status', NEW.to_status,
      'event_at', NEW.event_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_status_events_au AFTER UPDATE ON order_status_events FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_status_events', 'U',
    JSON_OBJECT('status_event_id', NEW.status_event_id),
    JSON_OBJECT('status_event_id', NEW.status_event_id, 'order_id', NEW.order_id,
      'from_status', NEW.from_status, 'to_status', NEW.to_status,
      'event_at', NEW.event_at, 'is_deleted', NEW.is_deleted)
  );
END$$

CREATE TRIGGER trg_status_events_ad AFTER DELETE ON order_status_events FOR EACH ROW
BEGIN
  INSERT INTO cdc_events (source_table, op, pk_json, row_json)
  VALUES (
    'order_status_events', 'D',
    JSON_OBJECT('status_event_id', OLD.status_event_id),
    JSON_OBJECT('status_event_id', OLD.status_event_id, 'order_id', OLD.order_id,
      'from_status', OLD.from_status, 'to_status', OLD.to_status,
      'event_at', OLD.event_at, 'is_deleted', OLD.is_deleted)
  );
END$$

DELIMITER ;
