"""
Tests for Catalog metadata registry structure and completeness.
"""

import os

from scripts.validate_catalog import catalog_path, validate_catalog_file


def test_catalog_json_validity() -> None:
    """Verify catalog/catalog.json passes metadata validation rules."""
    assert os.path.exists(catalog_path), f"Catalog file missing at {catalog_path}"
    errors = validate_catalog_file(catalog_path)
    assert errors == [], f"Catalog errors: {errors}"


def test_catalog_missing_file() -> None:
    """Verify validate_catalog_file reports error when file path is invalid."""
    errors = validate_catalog_file("/non/existent/catalog.json")
    assert len(errors) == 1
    assert "not found" in errors[0]
