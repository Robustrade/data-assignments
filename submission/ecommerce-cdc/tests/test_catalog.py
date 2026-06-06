"""Catalog tests: entries exist for every lake & warehouse dataset with metadata."""

import json
import os

from scripts.validate_catalog import REQUIRED_DATASETS, REQUIRED_FIELDS, validate

CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "catalog",
    "catalog.json",
)


def _catalog():
    with open(CATALOG_PATH) as f:
        return json.load(f)


def test_catalog_validates():
    assert validate() == []


def test_every_required_dataset_present():
    names = {d["name"] for d in _catalog()["datasets"]}
    for required in REQUIRED_DATASETS:
        assert required in names


def test_each_entry_has_required_fields():
    for entry in _catalog()["datasets"]:
        for field in REQUIRED_FIELDS:
            assert entry.get(field), f"{entry.get('name')} missing {field}"


def test_layers_are_lake_or_warehouse():
    for entry in _catalog()["datasets"]:
        assert entry["layer"] in {"lake", "warehouse"}


def test_catalog_matches_live_warehouse_tables(ingested):
    live = {row[0] for row in ingested.execute("SHOW TABLES").fetchall()}
    for entry in _catalog()["datasets"]:
        assert entry["name"] in live, f"{entry['name']} not found in lakehouse"
