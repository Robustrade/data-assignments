-- ============================================================
-- Warehouse schema: current-state snapshots + SCD2 history
-- for wallet and transfer (the two entities restore/time-travel
-- realistically needs). Other entities are snapshot-only.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ---------------------------------------------
-- Current-state snapshot tables (1 row per entity, latest state)
-- ---------------------------------------------
CREATE TABLE warehouse.customer (
    id          BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    is_deleted  BOOLEAN NOT NULL
);

CREATE TABLE warehouse.wallet (
    id           BIGINT PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    currency     TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    is_deleted   BOOLEAN NOT NULL
);

CREATE TABLE warehouse.transfer (
    id                BIGINT PRIMARY KEY,
    source_wallet_id  BIGINT NOT NULL,
    dest_wallet_id    BIGINT NOT NULL,
    amount            NUMERIC(18, 2) NOT NULL,
    status            TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    settled_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL,
    is_deleted        BOOLEAN NOT NULL
);

CREATE TABLE warehouse.wallet_balance_history (
    id             BIGINT PRIMARY KEY,
    wallet_id      BIGINT NOT NULL,
    balance_after  NUMERIC(18, 2) NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    is_deleted     BOOLEAN NOT NULL
);

CREATE TABLE warehouse.payment_attempt (
    id              BIGINT PRIMARY KEY,
    transfer_id     BIGINT NOT NULL,
    attempt_no      INT NOT NULL,
    status          TEXT NOT NULL,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    is_deleted      BOOLEAN NOT NULL
);

-- ---------------------------------------------
-- SCD2 history tables — wallet and transfer only.
-- valid_to = NULL means "currently valid" (is_current = true).
-- ---------------------------------------------
CREATE TABLE warehouse.wallet_history (
    surrogate_id  BIGSERIAL PRIMARY KEY,
    id            BIGINT NOT NULL,
    customer_id   BIGINT NOT NULL,
    currency      TEXT NOT NULL,
    status        TEXT NOT NULL,
    is_deleted    BOOLEAN NOT NULL,
    valid_from    TIMESTAMPTZ NOT NULL,
    valid_to      TIMESTAMPTZ,
    is_current    BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_wallet_history_id_current ON warehouse.wallet_history(id, is_current);

CREATE TABLE warehouse.transfer_history (
    surrogate_id      BIGSERIAL PRIMARY KEY,
    id                BIGINT NOT NULL,
    source_wallet_id  BIGINT NOT NULL,
    dest_wallet_id    BIGINT NOT NULL,
    amount            NUMERIC(18, 2) NOT NULL,
    status            TEXT NOT NULL,
    settled_at        TIMESTAMPTZ,
    is_deleted        BOOLEAN NOT NULL,
    valid_from        TIMESTAMPTZ NOT NULL,
    valid_to          TIMESTAMPTZ,
    is_current        BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_transfer_history_id_current ON warehouse.transfer_history(id, is_current);

-- ---------------------------------------------
-- Loader watermark: tracks last lake record processed per table,
-- keyed by (table, pk, updated_at) so replays are idempotent.
-- ---------------------------------------------
CREATE TABLE warehouse.load_log (
    table_name  TEXT NOT NULL,
    pk          BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, pk, updated_at)
);
