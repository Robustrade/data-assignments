-- ============================================================================
-- CDC Lakehouse Reliability Assignment — Source Schema
-- Domain: Wallet / Payments / Transfers
-- Engine: PostgreSQL (chosen for native logical replication / WAL-based CDC)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ENUM TYPES (enum-like fields requirement)
-- ----------------------------------------------------------------------------
CREATE TYPE wallet_status AS ENUM ('ACTIVE', 'FROZEN', 'CLOSED');
CREATE TYPE transaction_type AS ENUM ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'REFUND');
CREATE TYPE transaction_status AS ENUM ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED');
CREATE TYPE payment_attempt_status AS ENUM ('INITIATED', 'AUTHORIZED', 'CAPTURED', 'DECLINED', 'ERROR');

-- ----------------------------------------------------------------------------
-- STRONG ENTITY: customers
-- Independent existence, has its own natural identity (email), not owned by
-- any other table.
-- ----------------------------------------------------------------------------
CREATE TABLE customers (
    customer_id     BIGSERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    full_name       VARCHAR(255) NOT NULL,
    country_code    CHAR(2) NOT NULL,
    is_verified     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_country ON customers(country_code);

COMMENT ON TABLE customers IS
  'Strong entity. System of record for platform users. Independent lifecycle — a customer can exist with zero wallets.';
COMMENT ON COLUMN customers.email IS
  'Natural key candidate; unique, drives login/identity. Immutable in practice — treat rename/type-change as breaking.';

-- ----------------------------------------------------------------------------
-- STRONG ENTITY: wallets
-- Has its own identity (wallet_id), but existence is dependent on a customer
-- via FK (cannot exist without a customer). Modeled as strong because a
-- wallet has independent business meaning, its own balance, and its own
-- lifecycle events, distinct from a pure attribute of the customer.
-- ----------------------------------------------------------------------------
CREATE TABLE wallets (
    wallet_id       BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customers(customer_id),
    currency_code   CHAR(3) NOT NULL,
    balance         NUMERIC(18,2) NOT NULL DEFAULT 0.00,
    status          wallet_status NOT NULL DEFAULT 'ACTIVE',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_wallet_balance_nonneg CHECK (balance >= 0)
);

CREATE INDEX idx_wallets_customer_id ON wallets(customer_id);
CREATE INDEX idx_wallets_status ON wallets(status);
CREATE UNIQUE INDEX uq_wallet_customer_currency ON wallets(customer_id, currency_code);

COMMENT ON TABLE wallets IS
  'Strong entity. One customer may hold multiple wallets (one per currency). Balance is authoritative current state — invariant: balance >= 0.';
COMMENT ON COLUMN wallets.balance IS
  'Authoritative current balance. Must equal SUM of applied balance_history deltas — validated in warehouse parity checks.';

-- ----------------------------------------------------------------------------
-- STRONG ENTITY: transactions
-- Independently meaningful record of money movement; own identity and
-- lifecycle (PENDING -> COMPLETED/FAILED -> REVERSED).
-- ----------------------------------------------------------------------------
CREATE TABLE transactions (
    transaction_id      BIGSERIAL PRIMARY KEY,
    source_wallet_id    BIGINT REFERENCES wallets(wallet_id),
    destination_wallet_id BIGINT REFERENCES wallets(wallet_id),
    type                transaction_type NOT NULL,
    status              transaction_status NOT NULL DEFAULT 'PENDING',
    amount              NUMERIC(18,2) NOT NULL,
    currency_code       CHAR(3) NOT NULL,
    initiated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at          TIMESTAMPTZ,
    failure_reason      VARCHAR(500),
    CONSTRAINT chk_txn_amount_positive CHECK (amount > 0),
    CONSTRAINT chk_txn_settled_after_init CHECK (settled_at IS NULL OR settled_at >= initiated_at),
    CONSTRAINT chk_txn_wallets_not_same CHECK (
        source_wallet_id IS NULL OR destination_wallet_id IS NULL
        OR source_wallet_id <> destination_wallet_id
    )
);

CREATE INDEX idx_txn_source_wallet ON transactions(source_wallet_id);
CREATE INDEX idx_txn_dest_wallet ON transactions(destination_wallet_id);
CREATE INDEX idx_txn_status ON transactions(status);
CREATE INDEX idx_txn_initiated_at ON transactions(initiated_at);

COMMENT ON TABLE transactions IS
  'Strong entity. Records every money-movement event. Nullable wallet FKs allow one-sided flows (external deposit/withdrawal). Business invariant: settled_at >= initiated_at.';
COMMENT ON COLUMN transactions.failure_reason IS
  'Nullable — populated only when status = FAILED. Free-text diagnostic, not used for logic.';

-- ----------------------------------------------------------------------------
-- WEAK ENTITY: transaction_line_items
-- Has no independent existence or identity outside its parent transaction;
-- PK is composite (transaction_id, line_number) — cannot exist without the
-- owning transaction row.
-- ----------------------------------------------------------------------------
CREATE TABLE transaction_line_items (
    transaction_id  BIGINT NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    line_number     SMALLINT NOT NULL,
    description     VARCHAR(255) NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    fee_flag        BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (transaction_id, line_number),
    CONSTRAINT chk_line_amount_nonzero CHECK (amount <> 0)
);

CREATE INDEX idx_line_items_txn ON transaction_line_items(transaction_id);

COMMENT ON TABLE transaction_line_items IS
  'Weak entity. Existence entirely dependent on parent transaction (composite PK, ON DELETE CASCADE). Business invariant: SUM(amount) per transaction_id must equal parent transactions.amount.';

-- ----------------------------------------------------------------------------
-- WEAK ENTITY: balance_history
-- Append-oriented ledger of every balance-affecting event on a wallet.
-- No independent identity outside the owning wallet + transaction pair.
-- ----------------------------------------------------------------------------
CREATE TABLE balance_history (
    history_id      BIGSERIAL,
    wallet_id       BIGINT NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    transaction_id  BIGINT REFERENCES transactions(transaction_id),
    delta_amount    NUMERIC(18,2) NOT NULL,
    balance_after   NUMERIC(18,2) NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (wallet_id, history_id),
    CONSTRAINT chk_balance_after_nonneg CHECK (balance_after >= 0)
);

CREATE INDEX idx_balance_history_wallet ON balance_history(wallet_id, recorded_at);
CREATE INDEX idx_balance_history_txn ON balance_history(transaction_id);

COMMENT ON TABLE balance_history IS
  'Weak entity. Append-only ledger, partitioned conceptually by wallet_id. Drives wallet.balance reconciliation and is the primary source for warehouse time-travel reconstruction.';

-- ----------------------------------------------------------------------------
-- SUMMARY OF ENTITY INVARIANTS (for design doc reference)
-- ----------------------------------------------------------------------------
-- 1. wallets.balance >= 0                                     (chk_wallet_balance_nonneg)
-- 2. transactions.amount > 0                                  (chk_txn_amount_positive)
-- 3. transactions.settled_at >= transactions.initiated_at     (chk_txn_settled_after_init)
-- 4. SUM(transaction_line_items.amount) == transactions.amount (app/warehouse-level, cross-row)
-- 5. wallets.balance == last balance_history.balance_after per wallet (reconciliation)
-- 6. status transitions: PENDING -> {COMPLETED, FAILED} -> REVERSED only (app/warehouse-level)
