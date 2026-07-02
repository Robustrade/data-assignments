from __future__ import annotations

import json
from pathlib import Path


REQUIRED_DATASETS = {
    "raw_contract.kafka_cdc_event_shape",
    "silver.ticketing_cdc_events",
    "silver.reservations_current",
    "silver.reservation_items_current",
    "silver.payments_current",
    "silver.reservations_history",
    "WH_TICKETING_CURRENT.RESERVATIONS_CURRENT",
    "WH_TICKETING_CURRENT.RESERVATION_ITEMS_CURRENT",
    "WH_TICKETING_CURRENT.PAYMENTS_CURRENT",
    "WH_TICKETING_HISTORY.RESERVATIONS_HISTORY",
    "ops.schema_failures",
}


def main() -> int:
    path = Path(__file__).resolve().parents[1] / "catalog" / "catalog.json"
    catalog = json.loads(path.read_text())
    names = {dataset["name"] for dataset in catalog["datasets"]}
    missing = sorted(REQUIRED_DATASETS - names)

    if missing:
        print(f"Missing catalog datasets: {missing}")
        return 1

    print("Catalog metadata passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
