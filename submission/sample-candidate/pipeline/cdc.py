import datetime
from source.models import SCHEMA_CONTRACT

class SchemaMismatchException(Exception): pass

class CDCPipeline:
    def __init__(self, source_conn, lake_conn):
        self.source_conn = source_conn
        self.lake_conn = lake_conn
        self.high_watermarks = {table: datetime.datetime.min for table in SCHEMA_CONTRACT.keys()}

    def verify_schema_contract(self):
        """Active safety sentinel. Throws exception if upstream columns are corrupted."""
        for table_name, expected_cols in SCHEMA_CONTRACT.items():
            actual_cols = self.source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            actual_col_names = [col[1] for col in actual_cols]
            
            for col in expected_cols:
                if col not in actual_col_names:
                    raise SchemaMismatchException(
                        f"CRITICAL DRIFT DETECTED: Column '{col}' is missing on source table '{table_name}'!"
                    )

    def ingest_table_changes(self, table_name: str, lsn: int):
        """Pulls incremental change logs past the last recorded high-watermark."""
        self.verify_schema_contract()
        now = datetime.datetime.now()
        
        time_col = "created_at" if table_name == "transactions" else "updated_at"
        cols_str = ", ".join(SCHEMA_CONTRACT[table_name])
        
        changes = self.source_conn.execute(f"""
            SELECT {cols_str} FROM {table_name} WHERE {time_col} > ? ORDER BY {time_col} ASC
        """, (self.high_watermarks[table_name],)).fetchall()
        
        if not changes:
            return
            
        for row in changes:
            # Map mutations safely. If it's the initial pull, flag as Insert; otherwise Update
            op_type = 'I' if self.high_watermarks[table_name] == datetime.datetime.min else 'U'
            placeholders = ", ".join(["?"] * (len(row) + 3))
            
            self.lake_conn.execute(f"""
                INSERT INTO lake_{table_name}_history VALUES ({placeholders})
            """, (*row, op_type, now, lsn))
            
        self.high_watermarks[table_name] = max([row[-1] for row in changes])