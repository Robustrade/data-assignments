from scripts.validate_catalog import load_catalog, validate, expected_dataset_names


def test_committed_catalog_is_valid():
    """The actual catalog/catalog.json in this repo should always pass."""
    catalog = load_catalog()
    assert validate(catalog) == []


def test_every_expected_dataset_is_registered():
    catalog = load_catalog()
    registered = {d["name"] for d in catalog["datasets"]}
    assert expected_dataset_names() <= registered


def test_missing_dataset_is_detected():
    broken = {"datasets": []}
    problems = validate(broken)
    assert any("lake_cdc_events" in p for p in problems)
    assert any("wh_customers" in p for p in problems)


def test_incomplete_metadata_is_detected():
    broken = {
        "datasets": [
            {
                "name": "wh_customers",
                "layer": "warehouse",
                "description": "",  # empty - should be flagged
                "owner": "data-platform",
                "consumers": ["analytics"],
                "update_cadence": "near real-time",
                "schema": {"customer_id": "VARCHAR"},  # missing most columns
            }
        ]
    }
    problems = validate(broken)
    assert any("description" in p for p in problems)
    assert any("missing column" in p for p in problems)
