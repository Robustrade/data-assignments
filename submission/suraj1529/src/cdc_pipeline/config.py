from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "runtime"
SOURCE_DB = RUNTIME / "source.db"
WAREHOUSE_DB = RUNTIME / "warehouse.db"
LAKE_FILE = RUNTIME / "lake" / "cdc_events.jsonl"
CHECKPOINT_FILE = RUNTIME / "checkpoint.json"
VALIDATION_FILE = RUNTIME / "validation_results.jsonl"
SCHEMA_FILE = ROOT / "catalog" / "source_contract.json"

TABLES = ["customers", "products", "orders", "order_items", "payments"]
