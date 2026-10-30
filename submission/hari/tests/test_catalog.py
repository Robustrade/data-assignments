"""Catalog and the catalog CI script (CAT-01 … CAT-03)."""

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

HARI = Path(__file__).resolve().parent.parent
REPO = HARI.parent.parent
CATALOG = HARI / "catalog" / "catalog.json"
SCRIPT = HARI / "scripts" / "validate_catalog.py"


@pytest.fixture(scope="module")
def checker():
    spec = importlib.util.spec_from_file_location("validate_catalog", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def catalog():
    return json.loads(CATALOG.read_text(encoding="utf-8"))


def test_cat_01_every_dataset_has_an_entry(platform, checker, catalog):
    """CAT-01: every lake and warehouse dataset (table / view / history) is in the catalog."""
    live = checker.live_datasets(platform.conn)
    names = {d["name"] for d in catalog["datasets"]}
    assert set(live) == names
    assert len(names) == 19  # 1 lake + 9 views + 9 history tables
    assert len(catalog["datasets"]) == len(names)  # no duplicates


def test_cat_02_every_entry_is_complete(catalog):
    """CAT-02: layer, description, owner, consumers, cadence, PII, access, schema, example query."""
    for entry in catalog["datasets"]:
        name = entry["name"]
        for field in (
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
        ):
            assert entry.get(field), f"{name}: {field}"
        assert entry["layer"] in ("lake", "warehouse")
        assert entry["access"] in ("restricted", "general")
        assert isinstance(entry["consumers"], list) and entry["consumers"]
        assert set(entry["pii"]) == {"contains_pii", "columns", "treatment"}
        assert all(
            " — " in text for text in entry["schema"].values()
        ), f"{name}: schema entries are 'TYPE — description'"
        assert entry["example_query"].upper().startswith("SELECT")
    by_name = {d["name"]: d for d in catalog["datasets"]}
    assert (
        by_name["lake.cdc_events"]["access"] == "restricted"
        and by_name["lake.cdc_events"]["pii"]["contains_pii"]
    )
    assert by_name["warehouse.users"]["pii"]["columns"] == ["email_hash", "phone_hash"]
    assert not by_name["warehouse.orders"]["pii"]["contains_pii"]
    assert by_name["warehouse.orders_history"]["consumers"] == ["finance", "audit"]


def test_cat_02b_example_queries_run(platform, catalog):
    """The published example queries actually execute against the platform."""
    for entry in catalog["datasets"]:
        platform.conn.execute(entry["example_query"]).fetchall()


def test_cat_03_catalog_schema_matches_live_tables(platform, checker, catalog):
    """CAT-03: columns and types match the live tables; any drift is reported."""
    assert checker.validate(catalog, platform.conn) == []
    broken = copy.deepcopy(catalog)
    orders = next(d for d in broken["datasets"] if d["name"] == "warehouse.orders")
    orders["schema"]["total"] = "VARCHAR — wrong type"
    del orders["schema"]["status"]
    orders["schema"]["ghost"] = "INTEGER — not a column"
    broken["datasets"] = [
        d for d in broken["datasets"] if d["name"] != "warehouse.refunds"
    ]
    errors = " | ".join(checker.validate(broken, platform.conn))
    assert (
        "warehouse.orders.total: catalog says VARCHAR, live type is DECIMAL(18,2)"
        in errors
    )
    assert "warehouse.orders.status: column missing from catalog" in errors
    assert "warehouse.orders.ghost: catalog column does not exist" in errors
    assert "warehouse.refunds: dataset has no catalog entry" in errors


def run_script(*args):
    return subprocess.run(
        [sys.executable, "submission/hari/scripts/validate_catalog.py", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )


def test_validate_catalog_script_exit_codes(tmp_path, catalog):
    """The CI script exits 0 on the real catalog and 1 on a broken one."""
    ok = run_script()
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "19 datasets" in ok.stdout
    broken = copy.deepcopy(catalog)
    broken["datasets"][3]["schema"].pop(next(iter(broken["datasets"][3]["schema"])))
    path = tmp_path / "broken.json"
    path.write_text(json.dumps(broken))
    bad = run_script(str(path))
    assert bad.returncode == 1 and "column missing from catalog" in bad.stdout
