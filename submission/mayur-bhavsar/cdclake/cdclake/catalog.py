"""Catalog layer — publish the datasets, then prove the publication is true.

A catalog that is merely written down drifts within a sprint. Everything here
exists so that ``catalog/datasets.yml`` can be checked against reality:

* every dataset entry carries the metadata a consumer actually needs
  (owner, intended consumers, cadence, access path, PII flag, lineage);
* every warehouse entry names a dbt model that exists;
* every declared column set matches the physical relation;
* every physical relation is catalogued, so nothing ships undocumented.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cdclake import contracts as contracts_mod
from cdclake.config import CATALOG_FILE, ROOT
from cdclake.lake import LAKE_COLUMNS

REQUIRED_FIELDS: tuple[str, ...] = (
    "name",
    "layer",
    "owner",
    "description",
    "intended_consumers",
    "update_cadence",
    "storage",
    "location",
    "contains_pii",
    "lineage",
)

VALID_LAYERS = frozenset({"lake", "warehouse"})


@dataclass
class Dataset:
    raw: dict[str, Any]

    @property
    def name(self) -> str:
        return str(self.raw["name"])

    @property
    def layer(self) -> str:
        return str(self.raw.get("layer", ""))

    @property
    def relation(self) -> str:
        """Bare relation name, e.g. warehouse.customer_current ->
        customer_current."""
        return self.name.split(".", 1)[1]


@dataclass
class Catalog:
    owners: dict[str, Any] = field(default_factory=dict)
    column_groups: dict[str, list[str]] = field(default_factory=dict)
    datasets: list[Dataset] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path = CATALOG_FILE) -> Catalog:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return cls(
            owners=raw.get("owners", {}),
            column_groups=raw.get("column_groups", {}),
            datasets=[Dataset(d) for d in raw.get("datasets", [])],
        )

    def by_layer(self, layer: str) -> list[Dataset]:
        return [d for d in self.datasets if d.layer == layer]

    def get(self, name: str) -> Dataset | None:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        return None

    def declared_columns(self, dataset: Dataset) -> list[str]:
        """Resolve an entry's column list, expanding contract references."""
        if "columns" in dataset.raw:
            return list(dataset.raw["columns"])

        table = dataset.raw.get("columns_from_contract")
        group = dataset.raw.get("column_group")
        if not table or not group:
            raise ValueError(
                f"{dataset.name}: needs either 'columns' or both "
                "'columns_from_contract' and 'column_group'"
            )
        contract = contracts_mod.load_contract(str(table))
        return list(self.column_groups[str(group)]) + list(contract.column_names)


# ── validations ─────────────────────────────────────────────────────────────
def validate_structure(catalog: Catalog) -> list[str]:
    findings: list[str] = []
    seen: set[str] = set()

    for dataset in catalog.datasets:
        raw = dataset.raw
        name = raw.get("name", "<unnamed>")

        for field_name in REQUIRED_FIELDS:
            if field_name not in raw or raw[field_name] in (None, "", []):
                findings.append(f"{name}: missing required metadata '{field_name}'")

        if dataset.layer not in VALID_LAYERS:
            findings.append(
                f"{name}: layer must be one of {sorted(VALID_LAYERS)}, "
                f"got {dataset.layer!r}"
            )
        if not str(name).startswith(f"{dataset.layer}."):
            findings.append(
                f"{name}: name must be prefixed with its layer so consumers "
                "cannot confuse change history with current state"
            )
        if raw.get("owner") not in catalog.owners:
            findings.append(f"{name}: owner {raw.get('owner')!r} is not in owners:")
        if name in seen:
            findings.append(f"{name}: duplicate catalog entry")
        seen.add(str(name))

        try:
            if not catalog.declared_columns(dataset):
                findings.append(f"{name}: publishes no schema")
        except (ValueError, KeyError, FileNotFoundError) as exc:
            findings.append(f"{name}: cannot resolve columns — {exc}")

    return findings


def validate_lake_schema(catalog: Catalog) -> list[str]:
    dataset = catalog.get("lake.cdc_events")
    if dataset is None:
        return ["lake.cdc_events is not catalogued"]
    declared = catalog.declared_columns(dataset)
    if declared != list(LAKE_COLUMNS):
        return [
            "lake.cdc_events: catalogued schema does not match the physical "
            f"lake record. catalog={declared} actual={list(LAKE_COLUMNS)}"
        ]
    return []


def dbt_model_names(models_dir: Path = ROOT / "models") -> set[str]:
    return {path.stem for path in models_dir.rglob("*.sql")}


def validate_models_exist(
    catalog: Catalog, models_dir: Path = ROOT / "models"
) -> list[str]:
    """Every catalogued warehouse dataset must be a real dbt model, and every
    dbt model must be catalogued. Both directions matter: the first stops the
    catalog advertising something that does not exist, the second stops a model
    shipping undocumented."""
    models = dbt_model_names(models_dir)
    catalogued = {d.relation for d in catalog.by_layer("warehouse")}

    findings = [
        f"warehouse.{name}: catalogued but no dbt model of that name exists"
        for name in sorted(catalogued - models)
    ]
    findings += [
        f"{name}: dbt model exists but is not in the catalog"
        for name in sorted(models - catalogued)
    ]
    return findings


def validate_against_warehouse(catalog: Catalog, warehouse: Any) -> list[str]:
    """Compare catalogued schemas with the live DuckDB relations."""
    findings: list[str] = []
    live = {
        row["table_name"]: row["columns"]
        for row in warehouse.query(
            "select table_name, list(column_name) as columns "
            "from information_schema.columns "
            "where table_schema = 'main' group by table_name"
        )
    }
    for dataset in catalog.by_layer("warehouse"):
        relation = dataset.relation
        if relation not in live:
            findings.append(f"{dataset.name}: relation is missing from the warehouse")
            continue
        declared = set(catalog.declared_columns(dataset))
        actual = set(live[relation])
        if declared != actual:
            missing = sorted(declared - actual)
            extra = sorted(actual - declared)
            findings.append(
                f"{dataset.name}: schema drift — "
                f"catalogued but absent={missing}, present but uncatalogued={extra}"
            )
    return findings


def render_report(sections: Iterable[tuple[str, list[str]]]) -> tuple[str, bool]:
    lines: list[str] = []
    ok = True
    for title, findings in sections:
        if findings:
            ok = False
            lines.append(f"FAIL {title}")
            lines.extend(f"       - {f}" for f in findings)
        else:
            lines.append(f"OK   {title}")
    return "\n".join(lines), ok
