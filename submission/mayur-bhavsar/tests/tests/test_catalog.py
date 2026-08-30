"""Catalog tests: the metadata must describe the datasets that actually exist."""

from __future__ import annotations

import copy

from cdclake import catalog as catalog_mod
from cdclake.catalog import Catalog
from cdclake.config import TRACKED_TABLES


def test_catalog_lists_both_layers():
    catalog = Catalog.load()
    assert catalog.by_layer("lake"), "the lake is not catalogued"
    assert catalog.by_layer("warehouse"), "the warehouse is not catalogued"


def test_every_entry_carries_the_required_metadata():
    findings = catalog_mod.validate_structure(Catalog.load())
    assert not findings, "\n".join(findings)


def test_catalogued_lake_schema_matches_the_physical_record():
    assert not catalog_mod.validate_lake_schema(Catalog.load())


def test_catalog_and_dbt_models_agree_in_both_directions():
    """Undocumented models are as bad as documented ghosts."""
    assert not catalog_mod.validate_models_exist(Catalog.load())


def test_catalogued_schemas_match_the_live_warehouse(reference_platform):
    findings = catalog_mod.validate_against_warehouse(
        Catalog.load(), reference_platform.warehouse
    )
    assert not findings, "\n".join(findings)


def test_every_entity_has_both_a_history_and_a_current_dataset():
    catalog = Catalog.load()
    names = {d.name for d in catalog.datasets}
    for table in TRACKED_TABLES:
        assert f"warehouse.dim_{table}_history" in names
        assert f"warehouse.{table}_current" in names


def test_pii_is_flagged_where_it_exists():
    """A consumer must be able to tell, from the catalog alone, which datasets
    carry personal data — including the lake, which people forget."""
    catalog = Catalog.load()
    assert catalog.get("lake.cdc_events").raw["contains_pii"] is True
    assert catalog.get("warehouse.customer_current").raw["contains_pii"] is True
    assert catalog.get("warehouse.product_current").raw["contains_pii"] is False


def test_lineage_points_at_catalogued_datasets():
    catalog = Catalog.load()
    names = {d.name for d in catalog.datasets}
    for dataset in catalog.by_layer("warehouse"):
        for upstream in dataset.raw["lineage"]:
            assert upstream in names, f"{dataset.name}: unknown upstream {upstream}"


def test_validation_detects_an_undocumented_model():
    """Prove the check can fail — a catalog test that only ever passes is
    decoration."""
    catalog = Catalog.load()
    trimmed = Catalog(
        owners=catalog.owners,
        column_groups=catalog.column_groups,
        datasets=[d for d in catalog.datasets if d.name != "warehouse.payment_current"],
    )
    findings = catalog_mod.validate_models_exist(trimmed)
    assert any("payment_current" in f for f in findings)


def test_validation_detects_missing_metadata():
    catalog = Catalog.load()
    broken = copy.deepcopy(catalog.datasets[0].raw)
    broken.pop("owner")
    findings = catalog_mod.validate_structure(
        Catalog(
            owners=catalog.owners,
            column_groups=catalog.column_groups,
            datasets=[catalog_mod.Dataset(broken)],
        )
    )
    assert any("owner" in f for f in findings)
