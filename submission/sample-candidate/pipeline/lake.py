import duckdb
from source.models import SCHEMA_CONTRACT

def init_lake_store(source_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Creates the structural schema for an immutable append-only historical audit ledger."""
    lake_conn = duckdb.connect(':memory:')
    
    for table_name in SCHEMA_CONTRACT.keys():
        source_info = source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        col_definitions = [f"{col[1]} {col[2]}" for col in source_info]
        
        # Inject auditing metadata fields required to guarantee perfect recovery tracking
        col_definitions.extend([
            "_cdc_op_type CHAR(1)",
            "_cdc_extracted_at TIMESTAMP",
            "_cdc_lsn_version BIGINT"
        ])
        
        definitions_str = ", ".join(col_definitions)
        lake_conn.execute(f"CREATE TABLE lake_{table_name}_history ({definitions_str});")
        
    return lake_conn