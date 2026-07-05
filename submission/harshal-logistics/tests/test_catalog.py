"""
Tests — catalog metadata completeness and correctness.
"""

import json
import os
import pytest

from scripts.validate_catalog import validate, REQUIRED_DATASETS, REQUIRED_FIELDS


def test_catalog_file_is_valid():
    violations = validate()
    assert violations == [], f"Catalog validation failed: {violations}"


def test_catalog_has_all_required_datasets():
    # Read catalog content
    catalog_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "catalog",
        "catalog.json",
    )
    with open(catalog_path) as f:
        data = json.load(f)

    datasets = {d["name"]: d for d in data.get("datasets", [])}
    for name in REQUIRED_DATASETS:
        assert name in datasets
        assert datasets[name]["layer"] in ("lake", "warehouse")
        assert len(datasets[name]["schema"]) > 0
