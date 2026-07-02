from __future__ import annotations

import json
from pathlib import Path


def test_catalog_contains_lake_and_warehouse_datasets():
    path = Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
    catalog = json.loads(path.read_text())
    names = {dataset["name"] for dataset in catalog["datasets"]}

    assert "raw_contract.kafka_cdc_event_shape" in names
    assert "silver.ticketing_cdc_events" in names
    assert "silver.reservations_history" in names
    assert "WH_TICKETING_CURRENT.RESERVATIONS_CURRENT" in names
    assert "WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT" in names
    assert "WH_TICKETING_CURRENT.PAYMENTS_CURRENT" in names
    assert "WH_TICKETING_HISTORY.RESERVATIONS_HISTORY" in names


def test_catalog_entries_have_owner_layer_and_cadence():
    path = Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
    catalog = json.loads(path.read_text())

    for dataset in catalog["datasets"]:
        assert dataset["owner"]
        assert dataset["layer"]
        assert dataset["cadence"]
