import duckdb

# Immutable source-of-truth registry to guard downstream architecture
SCHEMA_CONTRACT = {
    "customers": ["customer_id", "name", "email", "status", "created_at", "updated_at"],
    "wallets": ["wallet_id", "customer_id", "balance", "currency", "status", "created_at", "updated_at"],
    "transactions": ["transaction_id", "sender_wallet_id", "receiver_wallet_id", "amount", "status", "created_at"]
}

def create_source_tables(conn: duckdb.DuckDBPyConnection):
    """Initializes a relational payments network domain with strict precision types."""
    # Strict Enum Constraints
    conn.execute("CREATE TYPE account_status AS ENUM ('active', 'suspended', 'inactive');")
    conn.execute("CREATE TYPE tx_status AS ENUM ('PENDING', 'SUCCESS', 'FAILED');")
    
    # Customers (Strong Entity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL UNIQUE,
            status account_status NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    
    # Wallets (Weak Entity dependent on Customers)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR REFERENCES customers(customer_id),
            balance DECIMAL(18, 2) NOT NULL CHECK (balance >= 0),
            currency VARCHAR(3) NOT NULL,
            status account_status NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    
    # Transactions (Append-Only Event Ledger)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR PRIMARY KEY,
            sender_wallet_id VARCHAR REFERENCES wallets(wallet_id),
            receiver_wallet_id VARCHAR REFERENCES wallets(wallet_id),
            amount DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
            status tx_status NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
    """)