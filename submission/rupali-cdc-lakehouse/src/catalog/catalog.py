from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


class Catalog:
    """
    Lightweight JSON catalog for lake and warehouse datasets.

    The catalog stores metadata such as:

    - dataset name
    - dataset type
    - schema
    """

    def __init__(self, catalog_path: str | Path):
        self.catalog_path = Path(catalog_path)
        self.datasets: List[Dict[str, Any]] = []

    def add_dataset(
        self,
        name: str,
        dataset_type: str,
        schema: Dict[str, Any],
    ) -> None:
        """
        Add a dataset to the catalog.

        Raises:
            ValueError: if a dataset with the same name already exists.
        """

        if not name:
            raise ValueError("Dataset name cannot be empty")

        if not dataset_type:
            raise ValueError("Dataset type cannot be empty")

        if not isinstance(schema, dict):
            raise ValueError("Dataset schema must be a dictionary")

        if self.get_dataset(name) is not None:
            raise ValueError(
                "Dataset already exists: {}".format(name)
            )

        dataset = {
            "dataset_name": name,
            "dataset_type": dataset_type,
            "schema": schema,
        }

        self.datasets.append(dataset)

    def get_dataset(
        self,
        name: str,
    ) -> Dict[str, Any] | None:
        """
        Return a dataset by name.

        Returns:
            Dataset metadata or None when the dataset does not exist.
        """

        for dataset in self.datasets:
            if dataset["dataset_name"] == name:
                return dataset

        return None

    def list_datasets(
        self,
        dataset_type: str,
    ) -> List[Dict[str, Any]]:
        """
        Return all datasets of the requested type.
        """

        return [
            dataset
            for dataset in self.datasets
            if dataset["dataset_type"] == dataset_type
        ]

    def save(self) -> None:
        """
        Persist the catalog as JSON.
        """

        self.catalog_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        payload = {
            "datasets": self.datasets,
        }

        with self.catalog_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                payload,
                file,
                indent=2,
                sort_keys=True,
            )

    @classmethod
    def load(
        cls,
        catalog_path: str | Path,
    ) -> "Catalog":
        """
        Load a catalog from a JSON file.
        """

        catalog_path = Path(catalog_path)

        with catalog_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            payload = json.load(file)

        catalog = cls(catalog_path)

        catalog.datasets = payload.get(
            "datasets",
            [],
        )

        return catalog