"""
Makes sure catalog/catalog.json actually reflects what the pipeline
produces - every lake and warehouse dataset must have an entry, and that
entry has to carry enough metadata for someone to actually use it without
reading pipeline code.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from source.models import EXPECTED_COLUMNS

CATALOG_PATH = PROJECT_ROOT / "catalog" / "catalog.json"
REQUIRED_FIELDS = ["name", "layer", "description", "owner", "consumers", "update_cadence", "schema"]


class CatalogValidationError(Exception):
    pass


def load_catalog() -> dict:
    with open(CATALOG_PATH) as f:
        return json.load(f)


def expected_dataset_names() -> set[str]:
    return {"lake_cdc_events"} | {f"wh_{table}" for table in EXPECTED_COLUMNS}


def validate(catalog: dict) -> list[str]:
    problems = []
    datasets = {d.get("name"): d for d in catalog.get("datasets", [])}

    missing = expected_dataset_names() - set(datasets)
    for name in sorted(missing):
        problems.append(f"no catalog entry for dataset '{name}'")

    for name, entry in datasets.items():
        for field in REQUIRED_FIELDS:
            if not entry.get(field):
                problems.append(f"'{name}': missing or empty required field '{field}'")

    # warehouse entries should at least document every source column plus the two
    # bookkeeping columns the warehouse always adds - catches the catalog going
    # stale after a schema change without anyone updating catalog.json to match
    for table in EXPECTED_COLUMNS:
        wh_name = f"wh_{table}"
        entry = datasets.get(wh_name)
        if not entry:
            continue
        documented = set(entry.get("schema", {}))
        expected = set(EXPECTED_COLUMNS[table]) | {"_cdc_seq", "_deleted"}
        undocumented = expected - documented
        if undocumented:
            problems.append(f"'{wh_name}': catalog schema is missing column(s) {sorted(undocumented)}")

    return problems


def enforce() -> None:
    catalog = load_catalog()
    problems = validate(catalog)
    if problems:
        raise CatalogValidationError(
            "Catalog validation failed:\n" + "\n".join(f"  - {p}" for p in problems)
        )


if __name__ == "__main__":
    try:
        enforce()
    except CatalogValidationError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"No catalog found at {CATALOG_PATH}", file=sys.stderr)
        sys.exit(1)

    print("Catalog is valid - every lake/warehouse dataset is registered with full metadata.")
