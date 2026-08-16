-- ============================================================
-- Seed data: initial state
-- Run this once after schema.sql to populate baseline rows.
-- ============================================================

-- Customers
INSERT INTO customer (name, email) VALUES
    ('Asha Rao', 'asha.rao@example.com'),
    ('Vikram Shah', 'vikram.shah@example.com'),
    ('Priya Nair', 'priya.nair@example.com');

-- Wallets (one each to start, INR)
INSERT INTO wallet (customer_id, currency, status) VALUES
    (1, 'INR', 'active'),
    (2, 'INR', 'active'),
    (3, 'INR', 'active');

-- Initial balance history (opening balances)
INSERT INTO wallet_balance_history (wallet_id, balance_after) VALUES
    (1, 5000.00),
    (2, 3000.00),
    (3, 10000.00);

-- A settled transfer: wallet 1 -> wallet 2
INSERT INTO transfer (source_wallet_id, dest_wallet_id, amount, status, settled_at) VALUES
    (1, 2, 500.00, 'settled', now());

INSERT INTO payment_attempt (transfer_id, attempt_no, status) VALUES
    (1, 1, 'succeeded');

-- Reflect the settled transfer in balance history
INSERT INTO wallet_balance_history (wallet_id, balance_after) VALUES
    (1, 4500.00),
    (2, 3500.00);

-- A pending transfer: wallet 3 -> wallet 1
INSERT INTO transfer (source_wallet_id, dest_wallet_id, amount, status) VALUES
    (3, 1, 1200.00, 'pending');

INSERT INTO payment_attempt (transfer_id, attempt_no, status) VALUES
    (2, 1, 'initiated');
