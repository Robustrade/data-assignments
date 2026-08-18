from src.cdc.events import create_event
from src.lake.writer import LakeWriter


def test_write_insert_event(tmp_path):
    writer = LakeWriter(tmp_path)

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

    written = writer.write(event)

    assert written is True

    events = writer.read_events("customers")

    assert len(events) == 1
    assert events[0]["operation"] == "INSERT"
    assert events[0]["primary_key"] == {"customer_id": 4}
    assert events[0]["after"]["email"] == "test@example.com"


def test_write_update_event(tmp_path):
    writer = LakeWriter(tmp_path)

    event = create_event(
        "customers",
        "UPDATE",
        {"customer_id": 1},
        {
            "customer_id": 1,
            "first_name": "Old",
            "last_name": "Name",
            "email": "old@example.com",
        },
        {
            "customer_id": 1,
            "first_name": "New",
            "last_name": "Name",
            "email": "new@example.com",
        },
    )

    writer.write(event)

    events = writer.read_events("customers")

    assert len(events) == 1
    assert events[0]["operation"] == "UPDATE"
    assert events[0]["before"]["email"] == "old@example.com"
    assert events[0]["after"]["email"] == "new@example.com"


def test_write_delete_event(tmp_path):
    writer = LakeWriter(tmp_path)

    event = create_event(
        "customers",
        "DELETE",
        {"customer_id": 1},
        {
            "customer_id": 1,
            "first_name": "Old",
            "last_name": "Name",
            "email": "old@example.com",
        },
        None,
    )

    writer.write(event)

    events = writer.read_events("customers")

    assert len(events) == 1
    assert events[0]["operation"] == "DELETE"
    assert events[0]["before"]["customer_id"] == 1
    assert events[0]["after"] is None


def test_duplicate_event_is_not_written_twice(tmp_path):
    writer = LakeWriter(tmp_path)

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

    first_write = writer.write(event)
    second_write = writer.write(event)

    assert first_write is True
    assert second_write is False

    events = writer.read_events("customers")

    assert len(events) == 1


def test_multiple_events_are_preserved(tmp_path):
    writer = LakeWriter(tmp_path)

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

    delete_event = create_event(
        "customers",
        "DELETE",
        {"customer_id": 4},
        {
            "customer_id": 4,
            "first_name": "Rupali Updated",
            "last_name": "Test",
            "email": "updated@example.com",
        },
        None,
    )

    writer.write(insert_event)
    writer.write(update_event)
    writer.write(delete_event)

    events = writer.read_events("customers")

    assert len(events) == 3

    assert events[0]["operation"] == "INSERT"
    assert events[1]["operation"] == "UPDATE"
    assert events[2]["operation"] == "DELETE"