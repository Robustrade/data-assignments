"""
Source schema for a wallet / payments / transfers domain.

Domain summary
--------------
Customers register on the platform, link payment methods, hold wallets, and
move money between wallets through transfers. Each transfer goes through
several state transitions, and every balance change on a wallet is recorded
in an immutable ledger.

Entity classification
---------------------
Strong entities (independent identity, can exist on their own):
  - customers      : a registered user of the platform
  - wallets        : a balance account owned by a customer; one customer can
                     hold multiple wallets (e.g. one per currency)
  - transfers      : a money movement between two wallets; has its own
                     business identity (transfer_id), references two wallets,
                     and exists even if downstream events are missing

Weak entities (lifecycle bound to a parent; cannot meaningfully exist alone):
  - payment_methods   : tied to a customer; remove the customer and the
                        method has no owner
  - transfer_events   : state-change records for a transfer; a transfer_event
                        only makes sense in the context of its transfer
  - ledger_entries    : append-only balance-change rows for a wallet; one
                        ledger_entry has no business meaning without its wallet

Mix of data types covered:
  - DECIMAL(18, 4)   amounts and balances (4 dp for FX-safe precision)
  - TIMESTAMP        created_at / updated_at / settled_at
  - VARCHAR enums    status, direction, currency, method_type
  - VARCHAR FKs      identifier strings (UUID-style)
  - Nullable cols    settled_at, reference, failure_reason, payment_method_id
  - DATE             registration_date

Indexes
-------
We add indexes that map to access patterns the CDC pipeline actually exercises:
  - foreign-key indexes (referential lookup + delete cascade scans)
  - updated_at indexes (incremental extraction by timestamp)
  - status indexes (filtered scans for pending/failed transfers)
  - composite (wallet_id, created_at) on ledger_entries (audit-by-wallet)

Important invariants (mirrored downstream in the warehouse validation layer):
  Source / system rules
    - all PKs are unique and non-null
    - FKs always point to a real parent row
    - enum-typed columns hold one of a fixed set of values
    - timestamp columns are non-null where required
  Business rules
    - wallet.balance >= 0 (no overdraft)
    - transfer.amount > 0
    - transfer.from_wallet_id != transfer.to_wallet_id (no self transfer)
    - transfer.settled_at >= transfer.created_at when settled
    - ledger_entries.signed_amount sums to wallet.balance per wallet
      (monetary integrity check)
    - transfer status follows allowed state machine:
        pending -> {settled, failed, reversed}
        failed  -> {pending} (retry)
        settled -> {reversed}
        reversed and failed are terminal otherwise
"""

from __future__ import annotations

import duckdb


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CUSTOMERS_DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id        VARCHAR PRIMARY KEY,
    full_name          VARCHAR NOT NULL,
    email              VARCHAR NOT NULL,
    country_code       VARCHAR NOT NULL,                   -- ISO-3166 alpha-2
    status             VARCHAR NOT NULL
                       CHECK (status IN ('active', 'suspended', 'closed')),
    registration_date  DATE NOT NULL,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
)
"""

PAYMENT_METHODS_DDL = """
CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id  VARCHAR PRIMARY KEY,
    customer_id        VARCHAR NOT NULL REFERENCES customers(customer_id),
    method_type        VARCHAR NOT NULL
                       CHECK (method_type IN ('card', 'bank', 'upi')),
    masked_identifier  VARCHAR NOT NULL,                   -- e.g. '****1234'
    is_default         BOOLEAN NOT NULL DEFAULT FALSE,
    status             VARCHAR NOT NULL
                       CHECK (status IN ('active', 'expired', 'revoked')),
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
)
"""

WALLETS_DDL = """
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id     VARCHAR PRIMARY KEY,
    customer_id   VARCHAR NOT NULL REFERENCES customers(customer_id),
    currency      VARCHAR NOT NULL                         -- ISO-4217
                  CHECK (currency IN ('USD', 'EUR', 'GBP', 'INR', 'JPY')),
    balance       DECIMAL(18, 4) NOT NULL DEFAULT 0
                  CHECK (balance >= 0),
    status        VARCHAR NOT NULL
                  CHECK (status IN ('active', 'frozen', 'closed')),
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL
)
"""

TRANSFERS_DDL = """
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id        VARCHAR PRIMARY KEY,
    from_wallet_id     VARCHAR NOT NULL REFERENCES wallets(wallet_id),
    to_wallet_id       VARCHAR NOT NULL REFERENCES wallets(wallet_id),
    payment_method_id  VARCHAR REFERENCES payment_methods(payment_method_id),
    amount             DECIMAL(18, 4) NOT NULL CHECK (amount > 0),
    currency           VARCHAR NOT NULL
                       CHECK (currency IN ('USD', 'EUR', 'GBP', 'INR', 'JPY')),
    status             VARCHAR NOT NULL
                       CHECK (status IN ('pending', 'settled',
                                         'failed', 'reversed')),
    reference          VARCHAR,                            -- nullable: external memo
    failure_reason     VARCHAR,                            -- nullable
    created_at         TIMESTAMP NOT NULL,
    settled_at         TIMESTAMP,                          -- null until settled
    CHECK (from_wallet_id <> to_wallet_id),
    CHECK (settled_at IS NULL OR settled_at >= created_at)
)
"""

TRANSFER_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS transfer_events (
    transfer_event_id  VARCHAR PRIMARY KEY,
    transfer_id        VARCHAR NOT NULL REFERENCES transfers(transfer_id),
    from_status        VARCHAR
                       CHECK (from_status IS NULL OR from_status IN
                              ('pending', 'settled', 'failed', 'reversed')),
    to_status          VARCHAR NOT NULL
                       CHECK (to_status IN ('pending', 'settled',
                                            'failed', 'reversed')),
    note               VARCHAR,
    occurred_at        TIMESTAMP NOT NULL
)
"""

LEDGER_ENTRIES_DDL = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    ledger_entry_id  VARCHAR PRIMARY KEY,
    wallet_id        VARCHAR NOT NULL REFERENCES wallets(wallet_id),
    transfer_id      VARCHAR REFERENCES transfers(transfer_id),
    direction        VARCHAR NOT NULL
                     CHECK (direction IN ('credit', 'debit')),
    amount           DECIMAL(18, 4) NOT NULL CHECK (amount > 0),
    signed_amount    DECIMAL(18, 4) NOT NULL,    -- + for credit, - for debit
    balance_after    DECIMAL(18, 4) NOT NULL CHECK (balance_after >= 0),
    occurred_at      TIMESTAMP NOT NULL,
    CHECK ((direction = 'credit' AND signed_amount =  amount) OR
           (direction = 'debit'  AND signed_amount = -amount))
)
"""

# Indexes that match real CDC / lookup access patterns.
# Note: PKs already produce a unique index automatically; we add the others.
INDEX_DDL: list[str] = [
    "CREATE INDEX IF NOT EXISTS ix_customers_updated_at "
    "ON customers(updated_at)",
    "CREATE INDEX IF NOT EXISTS ix_customers_status "
    "ON customers(status)",
    "CREATE INDEX IF NOT EXISTS ix_payment_methods_customer "
    "ON payment_methods(customer_id)",
    "CREATE INDEX IF NOT EXISTS ix_wallets_customer "
    "ON wallets(customer_id)",
    "CREATE INDEX IF NOT EXISTS ix_wallets_updated_at "
    "ON wallets(updated_at)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_status "
    "ON transfers(status)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_from_wallet "
    "ON transfers(from_wallet_id)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_to_wallet "
    "ON transfers(to_wallet_id)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_created_at "
    "ON transfers(created_at)",
    "CREATE INDEX IF NOT EXISTS ix_transfer_events_transfer "
    "ON transfer_events(transfer_id)",
    "CREATE INDEX IF NOT EXISTS ix_ledger_wallet_time "
    "ON ledger_entries(wallet_id, occurred_at)",
    "CREATE INDEX IF NOT EXISTS ix_ledger_transfer "
    "ON ledger_entries(transfer_id)",
]


# Tables in dependency order (parents before children) — used for create + drop.
TABLES_IN_ORDER: list[str] = [
    "customers",
    "payment_methods",
    "wallets",
    "transfers",
    "transfer_events",
    "ledger_entries",
]

PRIMARY_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "payment_methods": "payment_method_id",
    "wallets": "wallet_id",
    "transfers": "transfer_id",
    "transfer_events": "transfer_event_id",
    "ledger_entries": "ledger_entry_id",
}

# Allowed enum domains — mirrored in schema contracts and downstream validation.
ENUM_DOMAINS: dict[tuple[str, str], frozenset[str]] = {
    ("customers", "status"): frozenset({"active", "suspended", "closed"}),
    ("payment_methods", "method_type"): frozenset({"card", "bank", "upi"}),
    ("payment_methods", "status"): frozenset({"active", "expired", "revoked"}),
    ("wallets", "currency"): frozenset({"USD", "EUR", "GBP", "INR", "JPY"}),
    ("wallets", "status"): frozenset({"active", "frozen", "closed"}),
    ("transfers", "status"): frozenset(
        {"pending", "settled", "failed", "reversed"}
    ),
    ("transfers", "currency"): frozenset({"USD", "EUR", "GBP", "INR", "JPY"}),
    ("transfer_events", "to_status"): frozenset(
        {"pending", "settled", "failed", "reversed"}
    ),
    ("ledger_entries", "direction"): frozenset({"credit", "debit"}),
}

# Allowed status transitions for transfers (state machine).
ALLOWED_TRANSFER_TRANSITIONS: frozenset[tuple[str, str]] = frozenset({
    ("pending", "settled"),
    ("pending", "failed"),
    ("pending", "reversed"),
    ("failed", "pending"),       # retry
    ("settled", "reversed"),
})


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all source tables and indexes in dependency order."""
    for ddl in (
        CUSTOMERS_DDL,
        PAYMENT_METHODS_DDL,
        WALLETS_DDL,
        TRANSFERS_DDL,
        TRANSFER_EVENTS_DDL,
        LEDGER_ENTRIES_DDL,
    ):
        conn.execute(ddl)
    for ix in INDEX_DDL:
        conn.execute(ix)


def drop_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop all source tables in reverse dependency order. Useful in tests."""
    for table in reversed(TABLES_IN_ORDER):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
