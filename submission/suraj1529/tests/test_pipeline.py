import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cdc_pipeline.cdc import build_demo_events
from cdc_pipeline.config import RUNTIME, SOURCE_DB, WAREHOUSE_DB, SCHEMA_FILE, ROOT
from cdc_pipeline.source import init_source, schema_snapshot
from cdc_pipeline.pipeline import Pipeline
from cdc_pipeline.schema_guard import SchemaGuard, SchemaChangeError
from cdc_pipeline.warehouse import Warehouse

def clean():
    import shutil
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)

def test_insert_update_delete_and_replay():
    clean()
    init_source(SOURCE_DB, ROOT / "sql/source_schema.sql")
    observed = schema_snapshot(SOURCE_DB)
    events = build_demo_events()
    contract = json.loads(SCHEMA_FILE.read_text())
    result = Pipeline(contract).run(events, observed)
    assert any(x[1] == "applied" for x in result)

    # Replay should not mutate the state again.
    result2 = Pipeline(contract).run(events, observed)
    assert result2 == []

    wh = Warehouse(WAREHOUSE_DB)
    try:
        order = wh.con.execute("SELECT status FROM orders WHERE order_id=1001").fetchone()
        assert order["status"] == "PAID"
        assert wh.con.execute("SELECT 1 FROM products WHERE product_id=102").fetchone() is None
    finally:
        wh.close()

def test_incompatible_schema_stops_before_processing():
    clean()
    init_source(SOURCE_DB, ROOT / "sql/source_schema.sql")
    observed = schema_snapshot(SOURCE_DB)
    observed["orders"] = [c for c in observed["orders"] if c["name"] != "order_total"]
    contract = json.loads(SCHEMA_FILE.read_text())
    try:
        SchemaGuard(contract).validate(observed)
        assert False, "expected schema failure"
    except SchemaChangeError as exc:
        assert "order_total" in str(exc)
    assert not WAREHOUSE_DB.exists()

def test_scd2_reconstructs_previous_state():
    clean()
    init_source(SOURCE_DB, ROOT / "sql/source_schema.sql")
    contract = json.loads(SCHEMA_FILE.read_text())
    Pipeline(contract).run(build_demo_events(), schema_snapshot(SOURCE_DB))
    wh = Warehouse(WAREHOUSE_DB)
    try:
        old = wh.state_as_of("customers", "2026-09-17T09:00:00+00:00")
        new = wh.state_as_of("customers", "2026-09-17T11:00:00+00:00")
        assert old[0]["full_name"] == "Alice"
        assert new[0]["full_name"] == "Alice P"
    finally:
        wh.close()
