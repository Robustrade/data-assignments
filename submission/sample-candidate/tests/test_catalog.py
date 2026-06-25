"""
Tests — catalog metadata completeness and correctness.

Covers: file presence, all required datasets, required fields, layer labels,
schema presence, and that lake/warehouse datasets are correctly distinguished.
"""

import json
import os

import pytest

CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "catalog",
    "catalog.json",
)

REQUIRED_DATASETS = [
    "lake_cdc_events",
    "wh_customers",
    "wh_wallets",
    "wh_transactions",
]

REQUIRED_FIELDS = ["name", "layer", "description", "owner", "schema", "update_cadence"]


@pytest.fixture(scope="module")
def catalog() -> dict:
    with open(CATALOG_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def datasets(catalog: dict) -> dict[str, dict]:
    return {d["name"]: d for d in catalog.get("datasets", []) if "name" in d}


# ── file presence ─────────────────────────────────────────────────────────────


def test_catalog_file_exists():
    assert os.path.exists(CATALOG_PATH), f"catalog.json not found at {CATALOG_PATH}"


def test_catalog_file_is_valid_json():
    with open(CATALOG_PATH) as f:
        data = json.load(f)
    assert isinstance(data, dict)


def test_catalog_has_datasets_key(catalog: dict):
    assert "datasets" in catalog
    assert isinstance(catalog["datasets"], list)


# ── required datasets ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", REQUIRED_DATASETS)
def test_required_dataset_is_present(name: str, datasets: dict):
    assert name in datasets, f"Dataset '{name}' is missing from catalog"


def test_catalog_has_at_least_four_datasets(datasets: dict):
    assert len(datasets) >= 4


# ── required fields ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", REQUIRED_DATASETS)
@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_required_field_is_present_and_non_empty(name: str, field: str, datasets: dict):
    entry = datasets.get(name, {})
    assert field in entry, f"Dataset '{name}' is missing field '{field}'"
    assert entry[field], f"Dataset '{name}' field '{field}' is empty"


# ── layer labels ──────────────────────────────────────────────────────────────


def test_lake_cdc_events_is_labeled_as_lake(datasets: dict):
    assert datasets["lake_cdc_events"]["layer"] == "lake"


@pytest.mark.parametrize(
    "name",
    ["wh_customers", "wh_wallets", "wh_transactions"],
)
def test_warehouse_datasets_are_labeled_as_warehouse(name: str, datasets: dict):
    assert datasets[name]["layer"] == "warehouse"


# ── schema presence ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", REQUIRED_DATASETS)
def test_schema_field_is_a_non_empty_dict(name: str, datasets: dict):
    schema = datasets[name].get("schema", {})
    assert isinstance(schema, dict)
    assert len(schema) > 0, f"Dataset '{name}' has an empty schema"


def test_lake_schema_includes_operation_column(datasets: dict):
    assert "operation" in datasets["lake_cdc_events"]["schema"]


def test_lake_schema_includes_sequence_column(datasets: dict):
    assert "sequence" in datasets["lake_cdc_events"]["schema"]


def test_wh_customers_schema_includes_pk(datasets: dict):
    assert "customer_id" in datasets["wh_customers"]["schema"]


def test_wh_transactions_schema_includes_amount(datasets: dict):
    assert "amount" in datasets["wh_transactions"]["schema"]


# ── update cadence ────────────────────────────────────────────────────────────


def test_lake_update_cadence_is_real_time(datasets: dict):
    assert datasets["lake_cdc_events"]["update_cadence"] == "real-time"


@pytest.mark.parametrize(
    "name",
    ["wh_customers", "wh_wallets", "wh_transactions"],
)
def test_warehouse_update_cadence_is_near_real_time(name: str, datasets: dict):
    assert datasets[name]["update_cadence"] == "near-real-time"
