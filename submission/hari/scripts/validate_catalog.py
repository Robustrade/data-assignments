"""Compare catalog/catalog.json with the live lake and warehouse datasets.

Usage: python submission/hari/scripts/validate_catalog.py [catalog.json]
Builds a fresh platform in a temp dir; exits 1 on any missing dataset, missing/extra
column, or type mismatch.
"""

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from demo import Workbench

REQUIRED = (
    "name",
    "layer",
    "description",
    "owner",
    "consumers",
    "update_cadence",
    "pii",
    "access",
    "schema",
    "example_query",
)


def live_datasets(conn) -> dict[str, dict[str, str]]:
    """Every dataset consumers can see: the lake table, warehouse views and history tables."""
    out: dict[str, dict[str, str]] = {}
    rows = conn.execute(
        "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_schema IN ('lake', 'warehouse') ORDER BY table_schema, table_name, ordinal_position"
    ).fetchall()
    for schema, table, column, dtype in rows:
        if table.endswith("_current"):
            continue  # internal tables behind the consumer views
        out.setdefault(f"{schema}.{table}", {})[column] = dtype
    return out


def validate(catalog: dict, conn) -> list[str]:
    errors = []
    entries = {d.get("name"): d for d in catalog.get("datasets", [])}
    live = live_datasets(conn)
    for name in sorted(set(live) - set(entries)):
        errors.append(f"{name}: dataset has no catalog entry")
    for name in sorted(set(entries) - set(live)):
        errors.append(f"{name}: catalog entry has no live dataset")
    for name, entry in entries.items():
        for field in REQUIRED:
            if entry.get(field) in (None, "", [], {}):
                errors.append(f"{name}: missing {field}")
        if name not in live:
            continue
        described = {
            col: text.split(" — ")[0].strip()
            for col, text in entry.get("schema", {}).items()
        }
        for col, dtype in live[name].items():
            if col not in described:
                errors.append(f"{name}.{col}: column missing from catalog")
            elif described[col] != dtype:
                errors.append(
                    f"{name}.{col}: catalog says {described[col]}, live type is {dtype}"
                )
        for col in set(described) - set(live[name]):
            errors.append(f"{name}.{col}: catalog column does not exist")
    return errors


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else ROOT / "catalog" / "catalog.json"
    catalog = json.loads(path.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as tmp:
        wb = Workbench(tmp)
        try:
            errors = validate(catalog, wb.conn)
            checked = len(live_datasets(wb.conn))
        finally:
            wb.close()
    if errors:
        print(f"catalog check FAILED ({len(errors)} problem(s)):")
        for error in errors:
            print(f"  - {error}")
        return 1
    print(f"catalog check passed: {checked} datasets match the live schema")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
