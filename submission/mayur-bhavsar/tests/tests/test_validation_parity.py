"""Validation parity, checked mechanically rather than asserted in prose.

The parity checker reads contracts/*.json and models/schema.yml and pairs them
up. These tests prove it is doing real work: it passes on the committed state,
and it fails the moment a source rule loses its downstream twin.
"""

from __future__ import annotations

import copy

import pytest
import yaml

from cdclake import quality
from cdclake.config import TRACKED_TABLES


def test_every_published_constraint_has_a_warehouse_assertion():
    results = quality.check_validation_parity()
    failures = [r for r in results if not r.passed]

    assert results, "parity checker produced no checks at all"
    assert not failures, "\n".join(str(f) for f in failures)


def test_parity_covers_every_tracked_table():
    names = {r.name for r in quality.check_validation_parity()}
    for table in TRACKED_TABLES:
        assert any(f"parity::{table}_current" in name for name in names), table


def _write_schema(tmp_path, document):
    path = tmp_path / "schema.yml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


@pytest.fixture
def schema_document():
    return yaml.safe_load(quality.SCHEMA_YML.read_text(encoding="utf-8"))


def _model(document, name):
    return next(m for m in document["models"] if m["name"] == name)


def _column(document, model, column):
    return next(c for c in _model(document, model)["columns"] if c["name"] == column)


def test_parity_fails_when_a_not_null_assertion_is_removed(tmp_path, schema_document):
    document = copy.deepcopy(schema_document)
    column = _column(document, "customer_current", "email")
    column["data_tests"] = [t for t in column["data_tests"] if t != "not_null"]

    results = quality.check_validation_parity(_write_schema(tmp_path, document))
    failures = [r for r in results if not r.passed]

    assert any("email.not_null" in f.name for f in failures)


def test_parity_fails_when_an_enum_domain_drifts(tmp_path, schema_document):
    """Presence of an accepted_values test is not enough — the domain has to
    be the same set the source publishes."""
    document = copy.deepcopy(schema_document)
    column = _column(document, "payment_current", "status")
    for test in column["data_tests"]:
        if isinstance(test, dict) and "accepted_values" in test:
            test["accepted_values"]["arguments"]["values"] = ["INITIATED", "SETTLED"]

    results = quality.check_validation_parity(_write_schema(tmp_path, document))
    failures = [r for r in results if not r.passed]

    assert any("status.accepted_values" in f.name for f in failures)


def test_parity_fails_when_a_foreign_key_assertion_is_removed(
    tmp_path, schema_document
):
    document = copy.deepcopy(schema_document)
    column = _column(document, "order_item_current", "order_id")
    column["data_tests"] = [
        t
        for t in column["data_tests"]
        if not (isinstance(t, dict) and "relationships" in t)
    ]

    results = quality.check_validation_parity(_write_schema(tmp_path, document))
    failures = [r for r in results if not r.passed]

    assert any("order_id.relationships" in f.name for f in failures)


def test_parity_fails_when_a_unique_constraint_is_dropped(tmp_path, schema_document):
    document = copy.deepcopy(schema_document)
    column = _column(document, "product_current", "sku")
    column["data_tests"] = [t for t in column["data_tests"] if t != "unique"]

    results = quality.check_validation_parity(_write_schema(tmp_path, document))
    failures = [r for r in results if not r.passed]

    assert any("sku.unique" in f.name for f in failures)


def test_platform_checks_pass_on_a_full_run(reference_platform):
    results = quality.run_platform_checks(reference_platform)
    failures = [r for r in results if not r.passed]

    assert not failures, "\n".join(str(f) for f in failures)
    categories = {r.category for r in results}
    assert {
        "system",
        "completeness",
        "durability",
        "freshness",
        "validation-parity",
    } <= (categories)


def test_observed_freshness_meets_the_stated_objective(reference_platform):
    results = quality.check_freshness(reference_platform)
    assert results[0].passed, results[0].detail
