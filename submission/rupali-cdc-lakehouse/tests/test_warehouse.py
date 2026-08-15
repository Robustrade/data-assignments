from src.cdc.events import create_event
from src.warehouse.writer import WarehouseWriter


def test_insert_event_creates_current_record(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 4},
        None,
        {
            "customer_id": 4,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "test@example.com",
        },
    )

    writer.apply(event)

    customer = writer.get_record(
        "customers",
        {"customer_id": 4},
    )

    assert customer["customer_id"] == 4
    assert customer["first_name"] == "Rupali"
    assert customer["email"] == "test@example.com"


def test_update_event_replaces_current_record(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    insert_event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 4},
        None,
        {
            "customer_id": 4,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "test@example.com",
        },
    )

    update_event = create_event(
        "customers",
        "UPDATE",
        {"customer_id": 4},
        {
            "customer_id": 4,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "test@example.com",
        },
        {
            "customer_id": 4,
            "first_name": "Rupali Updated",
            "last_name": "Test",
            "email": "updated@example.com",
        },
    )

    writer.apply(insert_event)
    writer.apply(update_event)

    customer = writer.get_record(
        "customers",
        {"customer_id": 4},
    )

    assert customer["customer_id"] == 4
    assert customer["first_name"] == "Rupali Updated"
    assert customer["email"] == "updated@example.com"


def test_delete_event_removes_current_record(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    insert_event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 4},
        None,
        {
            "customer_id": 4,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "test@example.com",
        },
    )

    delete_event = create_event(
        "customers",
        "DELETE",
        {"customer_id": 4},
        {
            "customer_id": 4,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "test@example.com",
        },
        None,
    )

    writer.apply(insert_event)
    writer.apply(delete_event)

    customer = writer.get_record(
        "customers",
        {"customer_id": 4},
    )

    assert customer is None