import json
from pathlib import Path

def load_catalog(path: Path):
    return json.loads(path.read_text())

def validate_catalog(catalog):
    datasets = {x["name"]: x for x in catalog["datasets"]}
    required = {"lake.cdc_events", "warehouse.customers", "warehouse.products",
                "warehouse.orders", "warehouse.order_items", "warehouse.payments"}
    missing = required - set(datasets)
    if missing:
        raise AssertionError(f"Missing catalog datasets: {sorted(missing)}")
    return True
