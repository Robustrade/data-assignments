import sys
import shutil
from pathlib import Path
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

BASE = Path(__file__).resolve().parent.parent


@pytest.fixture
def clean_env(monkeypatch):
    """Reset data/checkpoints/lake dirs so each test starts from a clean slate."""
    import importlib

    for d in ["data", "checkpoints", "lake"]:
        p = BASE / d
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # reload modules so their module-level PATH constants (already correct,
    # since they're computed from __file__) see the fresh directories
    import source_db, schema_guard, cdc_extractor, warehouse_loader, validations
    importlib.reload(source_db)
    importlib.reload(schema_guard)
    importlib.reload(cdc_extractor)
    importlib.reload(warehouse_loader)
    importlib.reload(validations)

    yield {
        "source_db": source_db,
        "schema_guard": schema_guard,
        "cdc_extractor": cdc_extractor,
        "warehouse_loader": warehouse_loader,
        "validations": validations,
    }
