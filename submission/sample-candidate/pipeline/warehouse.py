from source.models import SCHEMA_CONTRACT

def project_warehouse_table(lake_conn, table_name: str, as_of_time) -> list:
    """Runs a late-arriving resilient window partition to flatten historical logs down into snapshots."""
    cols_str = ", ".join(SCHEMA_CONTRACT[table_name])
    pk_col = "transaction_id" if table_name == "transactions" else f"{table_name[:-1]}_id"
    
    query = f"""
        WITH partitioned_ledger AS (
            SELECT {cols_str}, _cdc_op_type,
                   ROW_NUMBER() OVER(
                       PARTITION BY {pk_col} 
                       ORDER BY _cdc_lsn_version DESC, _cdc_extracted_at DESC
                   ) as tracking_rank
            FROM lake_{table_name}_history
            WHERE _cdc_extracted_at <= ?
        )
        SELECT {cols_str}
        FROM partitioned_ledger
        WHERE tracking_rank = 1 AND _cdc_op_type != 'D'
    """
    return lake_conn.execute(query, (as_of_time,)).fetchall()