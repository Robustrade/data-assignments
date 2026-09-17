import json
import shutil

from .config import RUNTIME, SOURCE_DB, WAREHOUSE_DB, SCHEMA_FILE, ROOT
from .source import init_source, insert_seed_data, schema_snapshot
from .cdc import build_demo_events
from .pipeline import Pipeline
from .warehouse import Warehouse
from .validation import validate_warehouse
from .catalog import load_catalog, validate_catalog

def main():
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    init_source(SOURCE_DB, ROOT / "sql" / "source_schema.sql")
    insert_seed_data(SOURCE_DB)

    contract = json.loads(SCHEMA_FILE.read_text())
    observed = schema_snapshot(SOURCE_DB)

    result = Pipeline(contract).run(build_demo_events(), observed)

    wh = Warehouse(WAREHOUSE_DB)
    try:
        validate_warehouse(wh)
        print("CDC result:", result)
        print("Current orders:", [dict(r) for r in wh.con.execute("SELECT * FROM orders")])
        print("Historical customer state:",
              wh.state_as_of("customers", "2026-09-17T10:00:00+00:00"))
    finally:
        wh.close()

    validate_catalog(load_catalog(ROOT / "catalog" / "catalog.json"))
    print("Catalog validation: PASS")
    print("Validation: PASS")

if __name__ == "__main__":
    main()
