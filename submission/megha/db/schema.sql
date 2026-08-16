-- ============================================================
-- Source schema: wallet / payments / transfers domain
-- Includes updated_at + is_deleted on every table to support
-- polling-based CDC (see APPROACH.md).
-- ============================================================

CREATE TYPE wallet_status AS ENUM ('active', 'frozen', 'closed');
CREATE TYPE transfer_status AS ENUM ('pending', 'settled', 'failed', 'reversed');
CREATE TYPE payment_attempt_status AS ENUM ('initiated', 'succeeded', 'failed');

-- ---------------------------------------------
-- customer (strong entity)
-- ---------------------------------------------
CREATE TABLE customer (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted  BOOLEAN NOT NULL DEFAULT false
);

-- ---------------------------------------------
-- wallet (strong entity)
-- ---------------------------------------------
CREATE TABLE wallet (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL REFERENCES customer(id),
    currency     CHAR(3) NOT NULL,
    status       wallet_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted   BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_wallet_customer_id ON wallet(customer_id);
CREATE INDEX idx_wallet_updated_at ON wallet(updated_at);

-- ---------------------------------------------
-- transfer (strong entity)
-- ---------------------------------------------
CREATE TABLE transfer (
    id                BIGSERIAL PRIMARY KEY,
    source_wallet_id  BIGINT NOT NULL REFERENCES wallet(id),
    dest_wallet_id    BIGINT NOT NULL REFERENCES wallet(id),
    amount            NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    status            transfer_status NOT NULL DEFAULT 'pending',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted        BOOLEAN NOT NULL DEFAULT false,
    CHECK (source_wallet_id <> dest_wallet_id),
    CHECK (settled_at IS NULL OR settled_at >= created_at)
);

CREATE INDEX idx_transfer_source_wallet ON transfer(source_wallet_id);
CREATE INDEX idx_transfer_dest_wallet ON transfer(dest_wallet_id);
CREATE INDEX idx_transfer_updated_at ON transfer(updated_at);

-- ---------------------------------------------
-- wallet_balance_history (weak entity, depends on wallet)
-- ---------------------------------------------
CREATE TABLE wallet_balance_history (
    id            BIGSERIAL PRIMARY KEY,
    wallet_id     BIGINT NOT NULL REFERENCES wallet(id) ON DELETE CASCADE,
    balance_after NUMERIC(18, 2) NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted    BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_wbh_wallet_recorded ON wallet_balance_history(wallet_id, recorded_at);
CREATE INDEX idx_wbh_updated_at ON wallet_balance_history(updated_at);

-- ---------------------------------------------
-- payment_attempt (weak entity, depends on transfer)
-- ---------------------------------------------
CREATE TABLE payment_attempt (
    id              BIGSERIAL PRIMARY KEY,
    transfer_id     BIGINT NOT NULL REFERENCES transfer(id) ON DELETE CASCADE,
    attempt_no      INT NOT NULL,
    status          payment_attempt_status NOT NULL DEFAULT 'initiated',
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (transfer_id, attempt_no)
);

CREATE INDEX idx_pa_transfer_attempt ON payment_attempt(transfer_id, attempt_no);
CREATE INDEX idx_pa_updated_at ON payment_attempt(updated_at);

-- ---------------------------------------------
-- CDC checkpoint table (tracks last-processed cursor per source table)
-- ---------------------------------------------
CREATE TABLE cdc_checkpoint (
    table_name       TEXT PRIMARY KEY,
    last_updated_at  TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    last_id          BIGINT NOT NULL DEFAULT 0
);

INSERT INTO cdc_checkpoint (table_name) VALUES
    ('customer'), ('wallet'), ('transfer'),
    ('wallet_balance_history'), ('payment_attempt');