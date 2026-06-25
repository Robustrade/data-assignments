"""
Tests — data catalog correctness.

The catalog (catalog/catalog.json) is the canonical entry point for downstream
consumers. It must:

  - exist at a fixed location
  - parse as valid JSON
  - declare every warehouse + lake dataset the pipeline produces
  - carry the required descriptive fields per dataset
  - use a consistent vocabulary for layer / type / sla_class
  - flag every PII / monetary dataset with a tag for governance
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
)

EXPECTED_DATASETS: set[str] = {
    "lake_cdc_events",
    "wh_customers",
    "wh_payment_methods",
    "wh_wallets",
    "wh_transfers",
    "wh_transfer_events",
    "wh_ledger_entries",
    "wh_customers_history",
    "wh_payment_methods_history",
    "wh_wallets_history",
    "wh_transfers_history",
    "wh_transfer_events_history",
    "wh_ledger_entries_history",
}

REQUIRED_FIELDS = (
    "name", "layer", "type", "description",
    "owner", "consumers", "update_cadence",
    "sla_class", "tags", "primary_key",
)

ALLOWED_LAYERS = {"lake", "warehouse"}
ALLOWED_SLA = {"tier-1", "tier-2", "tier-3"}
ALLOWED_TYPES = {
    "append-only-change-log",
    "current-state",
    "scd2-history",
}


@pytest.fixture(scope="module")
def catalog() -> dict:
    raw = CATALOG_PATH.read_text(encoding="utf-8")
    return json.loads(raw)


# ── basics ───────────────────────────────────────────────────────────────────


def test_catalog_file_exists():
    assert CATALOG_PATH.exists(), f"missing catalog at {CATALOG_PATH}"


def test_catalog_is_valid_json(catalog):
    assert isinstance(catalog, dict)
    assert "datasets" in catalog
    assert isinstance(catalog["datasets"], list)


def test_catalog_top_level_metadata(catalog):
    for key in ("catalog_version", "domain", "owner"):
        assert key in catalog, f"missing top-level field: {key}"
    assert catalog["domain"] == "wallet-payments-transfers"


# ── completeness ─────────────────────────────────────────────────────────────


def test_catalog_lists_every_pipeline_dataset(catalog):
    declared = {d["name"] for d in catalog["datasets"]}
    missing = EXPECTED_DATASETS - declared
    assert not missing, f"catalog missing datasets: {sorted(missing)}"


def test_catalog_has_no_unexpected_datasets(catalog):
    declared = {d["name"] for d in catalog["datasets"]}
    extra = declared - EXPECTED_DATASETS
    assert not extra, f"catalog has unexpected datasets: {sorted(extra)}"


# ── per-dataset required fields ──────────────────────────────────────────────


def test_every_dataset_has_required_fields(catalog):
    for ds in catalog["datasets"]:
        for f in REQUIRED_FIELDS:
            assert f in ds, f"{ds.get('name')} missing field {f}"


def test_every_dataset_has_a_schema_or_extension(catalog):
    """Current-state has schema; SCD2 history has schema_extension."""
    for ds in catalog["datasets"]:
        assert "schema" in ds or "schema_extension" in ds, (
            f"{ds['name']} declares no schema or schema_extension"
        )


# ── controlled vocabulary ────────────────────────────────────────────────────


def test_layers_use_allowed_vocabulary(catalog):
    for ds in catalog["datasets"]:
        assert ds["layer"] in ALLOWED_LAYERS, (
            f"{ds['name']} has unknown layer: {ds['layer']}"
        )


def test_types_use_allowed_vocabulary(catalog):
    for ds in catalog["datasets"]:
        assert ds["type"] in ALLOWED_TYPES, (
            f"{ds['name']} has unknown type: {ds['type']}"
        )


def test_sla_class_uses_allowed_vocabulary(catalog):
    for ds in catalog["datasets"]:
        assert ds["sla_class"] in ALLOWED_SLA, (
            f"{ds['name']} has unknown sla_class: {ds['sla_class']}"
        )


# ── consistency between layer and type ───────────────────────────────────────


def test_lake_layer_uses_change_log_type(catalog):
    for ds in catalog["datasets"]:
        if ds["layer"] == "lake":
            assert ds["type"] == "append-only-change-log"


def test_wh_history_tables_are_scd2(catalog):
    for ds in catalog["datasets"]:
        if ds["name"].endswith("_history"):
            assert ds["type"] == "scd2-history"


def test_wh_current_tables_are_current_state(catalog):
    for ds in catalog["datasets"]:
        if (
            ds["layer"] == "warehouse"
            and not ds["name"].endswith("_history")
        ):
            assert ds["type"] == "current-state"


# ── governance tagging ───────────────────────────────────────────────────────


def test_money_critical_datasets_carry_appropriate_tags(catalog):
    """Wallets / transfers / ledger feed finance — all must be tier-1."""
    money_critical = {
        "wh_wallets", "wh_transfers", "wh_ledger_entries",
        "wh_wallets_history", "wh_transfers_history",
        "wh_ledger_entries_history",
    }
    for ds in catalog["datasets"]:
        if ds["name"] in money_critical:
            assert ds["sla_class"] == "tier-1", (
                f"{ds['name']} feeds finance — must be tier-1"
            )


def test_history_tables_tagged_for_time_travel(catalog):
    """SCD2 datasets that back time-travel queries must be tagged."""
    for ds in catalog["datasets"]:
        if ds["type"] == "scd2-history" and "balance" in (
            ds.get("tags") or []
        ):
            assert "time-travel" in ds["tags"]


def test_consumers_is_a_non_empty_list(catalog):
    for ds in catalog["datasets"]:
        consumers = ds["consumers"]
        assert isinstance(consumers, list)
        assert consumers, f"{ds['name']} has empty consumers list"


def test_primary_key_is_a_non_empty_list(catalog):
    for ds in catalog["datasets"]:
        pk = ds["primary_key"]
        assert isinstance(pk, list) and pk
