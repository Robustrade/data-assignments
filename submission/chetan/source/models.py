"""
Source relational schema definition for a Payments and Digital Wallet system.

Domain Overview:
- Customers open Wallets to store funds and execute Transactions with Merchants.
- Each Transaction generates double-entry Ledger Records for balance reconciliation.

Strong Entities:
  - customers: Independent user account profiles
  - wallets: Financial balance containers owned by customers
  - merchants: Business counterparties for transactions

Weak Entities:
  - transactions: Dependent payment lifecycle events tied to wallets/merchants
  - ledger_entries: Dependent double-entry audit records tied to transactions/wallets

Invariants & Constraints:
  - Wallet balance must be non-negative (balance >= 0)
  - Transaction amount must be positive (amount > 0)
  - Transaction fee must be non-negative (fee >= 0)
  - Status fields restricted to explicit enum sets
  - Timestamp ordering: settled_at must be >= created_at when present
"""

from __future__ import annotations

import duckdb

# Schema contract defining required columns per source table.
# Downstream CDC ingestion and schema safety checks validate against this contract.
SCHEMA_CONTRACT: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "name",
        "email",
        "status",
        "created_at",
        "updated_at",
    ],
    "wallets": [
        "wallet_id",
        "customer_id",
        "balance",
        "currency",
        "status",
        "created_at",
        "updated_at",
    ],
    "merchants": [
        "merchant_id",
        "name",
        "email",
        "status",
        "created_at",
    ],
    "transactions": [
        "transaction_id",
        "wallet_id",
        "merchant_id",
        "amount",
        "fee",
        "direction",
        "status",
        "reference",
        "created_at",
        "settled_at",
    ],
    "ledger_entries": [
        "entry_id",
        "transaction_id",
        "wallet_id",
        "entry_type",
        "amount",
        "balance_after",
        "created_at",
    ],
}


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create source relational tables with primary keys, foreign keys,
    check constraints, and performance indexes.
    """
    # 1. Customers (Strong Entity)
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

    # 2. Wallets (Strong Entity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_id    VARCHAR PRIMARY KEY,
            customer_id  VARCHAR NOT NULL REFERENCES customers(customer_id),
            balance      DECIMAL(18, 2) NOT NULL DEFAULT 0.00
                         CHECK (balance >= 0.00),
            currency     VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                         CHECK (status IN ('active', 'frozen', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # 3. Merchants (Strong Entity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS merchants (
            merchant_id  VARCHAR PRIMARY KEY,
            name         VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            status       VARCHAR NOT NULL
                         CHECK (status IN ('active', 'inactive')),
            created_at   TIMESTAMP NOT NULL
        )
    """)

    # 4. Transactions (Weak Entity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            wallet_id       VARCHAR NOT NULL REFERENCES wallets(wallet_id),
            merchant_id     VARCHAR REFERENCES merchants(merchant_id),
            amount          DECIMAL(18, 2) NOT NULL CHECK (amount > 0.00),
            fee             DECIMAL(18, 2) NOT NULL DEFAULT 0.00 CHECK (fee >= 0.00),
            direction       VARCHAR NOT NULL
                            CHECK (direction IN ('credit', 'debit')),
            status          VARCHAR NOT NULL
                            CHECK (status IN ('pending', 'settled', 'failed', 'reversed')),
            reference       VARCHAR,
            created_at      TIMESTAMP NOT NULL,
            settled_at      TIMESTAMP
        )
    """)

    # 5. Ledger Entries (Weak Entity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ledger_entries (
            entry_id        VARCHAR PRIMARY KEY,
            transaction_id  VARCHAR NOT NULL REFERENCES transactions(transaction_id),
            wallet_id       VARCHAR NOT NULL REFERENCES wallets(wallet_id),
            entry_type      VARCHAR NOT NULL
                            CHECK (entry_type IN ('debit', 'credit')),
            amount          DECIMAL(18, 2) NOT NULL CHECK (amount > 0.00),
            balance_after   DECIMAL(18, 2) NOT NULL CHECK (balance_after >= 0.00),
            created_at      TIMESTAMP NOT NULL
        )
    """)

    # Indexes supporting query performance and change lookup patterns
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_wallets_customer ON wallets(customer_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_transaction ON ledger_entries(transaction_id);"
    )
