"""Catalog metadata tests."""

import json
from pathlib import Path


def test_catalog_has_lake_and_warehouse():
    catalog_path = Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
    data = json.loads(catalog_path.read_text(encoding="utf-8"))
    layers = {d["layer"] for d in data["datasets"]}
    assert "lake" in layers
    assert "warehouse" in layers
