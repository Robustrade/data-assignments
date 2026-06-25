"""
Source schema for a payments/wallet system.

Domain: customers own wallets; wallets have transactions.

Strong entities : customers, wallets
Weak entities   : transactions (lifecycle tied to wallet)

Invariants:
- wallet.balance >= 0
- transaction.amount > 0
- status fields restricted to known enum values
- settled_at must be >= created_at when present
"""

import duckdb

# Expected columns per table — used by schema-contract checks.
SCHEMA_CONTRACT: dict[str, list[str]] = {
    "customers": ["customer_id", "name", "email", "status", "created_at", "updated_at"],
    "wallets": [
        "wallet_id",
        "customer_id",
        "balance",
        "currency",
        "status",
        "created_at",
        "updated_at",
    ],
    "transactions": [
        "transaction_id",
        "wallet_id",
        "amount",
        "direction",
        "status",
        "reference",
        "created_at",
        "settled_at",
    ],
}


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create source tables with constraints in the given DuckDB connection."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'suspended', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_id    VARCHAR PRIMARY KEY,
            customer_id  VARCHAR NOT NULL REFERENCES customers(customer_id),
            balance      DECIMAL(18, 2) NOT NULL DEFAULT 0.00
                     CHECK (balance >= 0),
            currency     VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'frozen', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            wallet_id       VARCHAR NOT NULL REFERENCES wallets(wallet_id),
            amount          DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
            direction       VARCHAR NOT NULL
                        CHECK (direction IN ('credit', 'debit')),
            status          VARCHAR NOT NULL
                        CHECK (status IN ('pending', 'settled', 'failed', 'reversed')),
            reference       VARCHAR,
            created_at      TIMESTAMP NOT NULL,
            settled_at      TIMESTAMP
        )
    """)
