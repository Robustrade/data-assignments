from src.cdc.events import create_event
from src.lake.writer import LakeWriter
from src.recovery.recovery import HistoricalRecovery


def test_recovery_rebuilds_current_state(tmp_path):
    lake_path = tmp_path / "lake"
    warehouse_path = tmp_path / "warehouse.duckdb"

    writer = LakeWriter(lake_path)

    insert_event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 1},
        None,
        {
            "customer_id": 1,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "old@example.com",
        },
    )

    update_event = create_event(
        "customers",
        "UPDATE",
        {"customer_id": 1},
        {
            "customer_id": 1,
            "first_name": "Rupali",
            "last_name": "Test",
            "email": "old@example.com",
        },
        {
            "customer_id": 1,
            "first_name": "Rupali Updated",
            "last_name": "Test",
            "email": "new@example.com",
        },
    )

    writer.write(insert_event)
    writer.write(update_event)

    recovery = HistoricalRecovery(
        lake_path,
        warehouse_path,
    )

    warehouse = recovery.replay("customers")

    customer = warehouse.get_record(
        "customers",
        {"customer_id": 1},
    )

    assert customer["first_name"] == "Rupali Updated"
    assert customer["email"] == "new@example.com"

    warehouse.close()


def test_recovery_replays_delete_event(tmp_path):
    lake_path = tmp_path / "lake"
    warehouse_path = tmp_path / "warehouse.duckdb"

    writer = LakeWriter(lake_path)

    insert_event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 1},
        None,
        {
            "customer_id": 1,
            "first_name": "Rupali",
        },
    )

    delete_event = create_event(
        "customers",
        "DELETE",
        {"customer_id": 1},
        {
            "customer_id": 1,
            "first_name": "Rupali",
        },
        None,
    )

    writer.write(insert_event)
    writer.write(delete_event)

    recovery = HistoricalRecovery(
        lake_path,
        warehouse_path,
    )

    warehouse = recovery.replay("customers")

    customer = warehouse.get_record(
        "customers",
        {"customer_id": 1},
    )

    assert customer is None

    warehouse.close()


def test_recovery_can_rebuild_state_at_timestamp(tmp_path):
    lake_path = tmp_path / "lake"
    warehouse_path = tmp_path / "warehouse.duckdb"

    writer = LakeWriter(lake_path)

    insert_event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 1},
        None,
        {
            "customer_id": 1,
            "first_name": "Rupali",
            "email": "old@example.com",
        },
    )

    update_event = create_event(
        "customers",
        "UPDATE",
        {"customer_id": 1},
        {
            "customer_id": 1,
            "first_name": "Rupali",
            "email": "old@example.com",
        },
        {
            "customer_id": 1,
            "first_name": "Rupali Updated",
            "email": "new@example.com",
        },
    )

    # Create deterministic timestamps.
    insert_event = insert_event.__class__(
        event_id=insert_event.event_id,
        table_name=insert_event.table_name,
        operation=insert_event.operation,
        primary_key=insert_event.primary_key,
        before=insert_event.before,
        after=insert_event.after,
        captured_at="2026-08-15T10:00:00+00:00",
    )

    update_event = update_event.__class__(
        event_id=update_event.event_id,
        table_name=update_event.table_name,
        operation=update_event.operation,
        primary_key=update_event.primary_key,
        before=update_event.before,
        after=update_event.after,
        captured_at="2026-08-15T10:05:00+00:00",
    )

    writer.write(insert_event)
    writer.write(update_event)

    recovery = HistoricalRecovery(
        lake_path,
        warehouse_path,
    )

    warehouse = recovery.replay(
        "customers",
        until="2026-08-15T10:00:00+00:00",
    )

    customer = warehouse.get_record(
        "customers",
        {"customer_id": 1},
    )

    assert customer["first_name"] == "Rupali"
    assert customer["email"] == "old@example.com"

    warehouse.close()