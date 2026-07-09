import sys
import duckdb
from source.models import create_source_tables
from pipeline.lake import init_lake_store
from pipeline.cdc import CDCPipeline, SchemaMismatchException

def run_contract_verification():
    print("Executing pre-flight platform safety contract scans...")
    src_db = duckdb.connect(':memory:')
    create_source_tables(src_db)
    lake_db = init_lake_store(src_db)
    
    pipeline = CDCPipeline(src_db, lake_db)
    
    try:
        pipeline.verify_schema_contract()
        print("Success: Source database validation is compliant with schemas.")
    except SchemaMismatchException as e:
        print(f"Verification Failure: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_contract_verification()