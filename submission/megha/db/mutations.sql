-- ============================================================
-- Mutations: simulates a "next batch" of real-world activity.
-- Run this AFTER the first CDC extraction pass, so you have
-- something new for the second pass to pick up.
-- ============================================================

-- Update: pending transfer (id=2) becomes settled
UPDATE transfer
SET status = 'settled', settled_at = now(), updated_at = now()
WHERE id = 2;

UPDATE payment_attempt
SET status = 'succeeded', updated_at = now()
WHERE transfer_id = 2 AND attempt_no = 1;

INSERT INTO wallet_balance_history (wallet_id, balance_after) VALUES
    (3, 8800.00),
    (1, 5700.00);

-- Insert: a new customer + wallet + transfer that fails
INSERT INTO customer (name, email) VALUES ('Rohit Verma', 'rohit.verma@example.com');
INSERT INTO wallet (customer_id, currency, status) VALUES (4, 'INR', 'active');

INSERT INTO transfer (source_wallet_id, dest_wallet_id, amount, status) VALUES
    (2, 4, 250.00, 'failed');

INSERT INTO payment_attempt (transfer_id, attempt_no, status, failure_reason) VALUES
    (3, 1, 'failed', 'insufficient_funds');

-- Soft-delete: wallet 4 gets closed and marked deleted (edge case for CDC)
UPDATE wallet
SET status = 'closed', is_deleted = true, updated_at = now()
WHERE id = 4;
