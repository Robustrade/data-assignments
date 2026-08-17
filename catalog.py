"""
Catalog / Access Layer.

Generates catalog/catalog.yml describing every lake and warehouse dataset:
name, layer, schema pointer, owner, intended consumers, update cadence, and
access path. In production this would be a Glue/Unity/DataHub catalog entry
registered automatically by the pipeline on each successful run; here we
regenerate the file idempotently each run as the local analogue.
"""
import yaml
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
CATALOG_PATH = BASE / "catalog" / "catalog.yml"

TABLES = ["customers", "wallets", "transactions", "transaction_line_items", "balance_history"]


def build_catalog() -> dict:
    datasets = []
    for t in TABLES:
        datasets.append({
            "name": f"lake.{t}",
            "layer": "lake",
            "path": f"lake/{t}/dt=YYYY-MM-DD/changes.jsonl",
            "format": "JSONL, append-only, one row per change event",
            "schema_ref": "src/source_db.py::SCHEMA_SQL",
            "owner": "data-platform-team",
            "intended_consumers": ["data engineers", "downstream lake consumers", "audit/replay tooling"],
            "update_cadence": "near real-time (per CDC extraction cycle)",
            "primary_key": None,
            "notes": "Full change history. Never updated or deleted in place.",
        })
        datasets.append({
            "name": f"warehouse.{t}_current",
            "layer": "warehouse",
            "path": f"data/warehouse.duckdb::{t}_current",
            "format": "DuckDB table, latest snapshot",
            "schema_ref": "src/warehouse_loader.py",
            "owner": "data-platform-team",
            "intended_consumers": ["analytics", "BI dashboards", "product teams"],
            "update_cadence": "near real-time (per warehouse load cycle)",
            "primary_key": "pk_key (composite business key)",
            "notes": "One row per current business entity.",
        })
        datasets.append({
            "name": f"warehouse.{t}_history",
            "layer": "warehouse",
            "path": f"data/warehouse.duckdb::{t}_history",
            "format": "DuckDB table, SCD2 versioned history",
            "schema_ref": "src/warehouse_loader.py",
            "owner": "data-platform-team",
            "intended_consumers": ["analytics (time travel)", "compliance/audit", "incident investigation"],
            "update_cadence": "near real-time (per warehouse load cycle)",
            "primary_key": "version_id",
            "notes": "Enables point-in-time reconstruction via valid_from/valid_to/is_current.",
        })
    return {"datasets": datasets}


def write_catalog():
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CATALOG_PATH.write_text(yaml.dump(build_catalog(), sort_keys=False))
    print(f"Catalog written to {CATALOG_PATH}")


if __name__ == "__main__":
    write_catalog()
