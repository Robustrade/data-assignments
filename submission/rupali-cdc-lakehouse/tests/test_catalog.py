from src.catalog.catalog import Catalog


def test_catalog_contains_lake_dataset(tmp_path):
    catalog = Catalog(tmp_path / "catalog.json")

    catalog.add_dataset(
        name="cdc_event_history",
        dataset_type="lake",
        schema={
            "event_id": "string",
            "table_name": "string",
            "operation": "string",
            "primary_key": "object",
            "before": "object",
            "after": "object",
            "captured_at": "string",
        },
    )

    catalog.save()

    loaded = Catalog.load(
        tmp_path / "catalog.json"
    )

    dataset = loaded.get_dataset(
        "cdc_event_history"
    )

    assert dataset is not None
    assert dataset["dataset_name"] == "cdc_event_history"
    assert dataset["schema"]["event_id"] == "string"


def test_catalog_contains_warehouse_datasets(tmp_path):
    catalog = Catalog(tmp_path / "catalog.json")

    warehouse_tables = [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
    ]

    for table in warehouse_tables:
        catalog.add_dataset(
            name=table,
            dataset_type="warehouse",
            schema={
                "primary_key": "defined"
            },
        )

    catalog.save()

    loaded = Catalog.load(
        tmp_path / "catalog.json"
    )

    for table in warehouse_tables:
        dataset = loaded.get_dataset(table)

        assert dataset is not None
        assert dataset["dataset_name"] == table
        assert dataset["schema"] is not None


def test_catalog_can_list_datasets_by_type(tmp_path):
    catalog = Catalog(tmp_path / "catalog.json")

    catalog.add_dataset(
        name="cdc_event_history",
        dataset_type="lake",
        schema={
            "event_id": "string"
        },
    )

    catalog.add_dataset(
        name="customers",
        dataset_type="warehouse",
        schema={
            "customer_id": "BIGINT"
        },
    )

    lake_datasets = catalog.list_datasets("lake")
    warehouse_datasets = catalog.list_datasets("warehouse")

    assert len(lake_datasets) == 1
    assert lake_datasets[0]["dataset_name"] == "cdc_event_history"

    assert len(warehouse_datasets) == 1
    assert warehouse_datasets[0]["dataset_name"] == "customers"


def test_catalog_rejects_duplicate_dataset(tmp_path):
    catalog = Catalog(tmp_path / "catalog.json")

    catalog.add_dataset(
        name="customers",
        dataset_type="warehouse",
        schema={
            "customer_id": "BIGINT"
        },
    )

    try:
        catalog.add_dataset(
            name="customers",
            dataset_type="warehouse",
            schema={
                "customer_id": "BIGINT"
            },
        )
        assert False, "Expected duplicate dataset to be rejected"
    except ValueError:
        assert True