import duckdb

def execute_quality_suite(lake_conn):
    print("Auditing active business rule metrics across layers...")
    
    # 1. Validation Parity Rule: Confirm non-negative wallet amounts
    negative_balances = lake_conn.execute("""
        SELECT COUNT(*) FROM lake_wallets_history WHERE balance < 0
    """).fetchone()[0]
    assert negative_balances == 0, "CRITICAL ERROR: Detected negative currency amounts inside historical records."
    
    # 2. Invariant Check: Enforce valid system status domains
    invalid_statuses = lake_conn.execute("""
        SELECT COUNT(*) FROM lake_customers_history WHERE status NOT IN ('active', 'suspended', 'inactive')
    """).fetchone()[0]
    assert invalid_statuses == 0, "CRITICAL ERROR: Malformed enum parameters leaked past ingestion walls."
    
    print("All active data quality assertions passed.")