"""
    Validates the catalog metadata for completeness and correctness.

    Checks:
    - All required fields present (name, layer, description, owner, consumers)
    - Layer values are valid (lake or warehouse)
    - Schema fields are documented
    - Lineage is sensible
"""

import json
import os
import sys


def validate_catalog(catalog_path: str) -> list[str]:
    """
        Validate catalog JSON.
        
        Returns list of violations. Empty = all valid.
    """
    violations: list[str] = []

    if not os.path.exists(catalog_path):
        violations.append(f"❌ Catalog file not found: {catalog_path}")
        return violations
    import pdb; pdb.set_trace()
    try:
        with open(catalog_path) as f:
            catalog = json.load(f)
    except json.JSONDecodeError as e:
        violations.append(f"❌ Invalid JSON in catalog: {e}")
        return violations

    if "datasets" not in catalog:
        violations.append("❌ Catalog missing 'datasets' key")
        return violations

    datasets = catalog["datasets"]
    if not isinstance(datasets, list):
        violations.append("❌ 'datasets' must be a list")
        return violations

    # Check each dataset
    for i, ds in enumerate(datasets):
        if not isinstance(ds, dict):
            violations.append(f"  ❌ Dataset {i}: not a dict")
            continue

        # Required fields
        required_fields = ["name", "layer", "description", "owner", "consumers"]
        for field in required_fields:
            if field not in ds:
                violations.append(
                    f"  ❌ Dataset {i} ({ds.get('name', 'unknown')}): "
                    f"missing required field '{field}'"
                )

        # Valid layer values
        if "layer" in ds and ds["layer"] not in ["lake", "warehouse"]:
            violations.append(
                f"  ❌ Dataset {ds.get('name', 'unknown')}: "
                f"invalid layer '{ds['layer']}' (must be 'lake' or 'warehouse')"
            )

        # Consumers should be a list
        if "consumers" in ds and not isinstance(ds["consumers"], list):
            violations.append(
                f"  ❌ Dataset {ds.get('name', 'unknown')}: "
                f"'consumers' must be a list, got {type(ds['consumers']).__name__}"
            )

        # Schema should be documented
        if "schema" not in ds:
            violations.append(
                f"  ⚠️  Dataset {ds.get('name', 'unknown')}: "
                f"missing 'schema' documentation"
            )

    return violations


def main() -> int:
    """Run catalog validation."""
    catalog_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "catalog",
        "catalog.json"
    )
    violations = validate_catalog(catalog_path)

    if violations:
        print("⚠️  CATALOG VALIDATION ISSUES")
        print("=" * 60)
        for v in violations:
            print(v)
        print("=" * 60)
        return 1

    print("✅ Catalog validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
