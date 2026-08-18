# from src.cdc.events import create_event


# def test_create_insert_event():
#     event = create_event(
#         "customers",
#         "INSERT",
#         {"customer_id": 4},
#         None,
#         {
#             "customer_id": 4,
#             "first_name": "Rupali",
#             "last_name": "Test",
#             "email": "test@example.com",
#         },
#     )

#     assert event.table_name == "customers"
#     assert event.operation == "INSERT"
#     assert event.primary_key == {"customer_id": 4}
#     assert event.before is None
#     assert event.after["customer_id"] == 4
#     assert event.after["email"] == "test@example.com"
#     assert event.event_id
#     assert event.captured_at


# def test_create_update_event():
#     event = create_event(
#         "customers",
#         "UPDATE",
#         {"customer_id": 1},
#         {
#             "customer_id": 1,
#             "first_name": "Old",
#             "last_name": "Name",
#             "email": "old@example.com",
#         },
#         {
#             "customer_id": 1,
#             "first_name": "New",
#             "last_name": "Name",
#             "email": "new@example.com",
#         },
#     )

#     assert event.table_name == "customers"
#     assert event.operation == "UPDATE"
#     assert event.primary_key == {"customer_id": 1}
#     assert event.before["email"] == "old@example.com"
#     assert event.after["email"] == "new@example.com"


# def test_create_delete_event():
#     event = create_event(
#         "customers",
#         "DELETE",
#         {"customer_id": 1},
#         {
#             "customer_id": 1,
#             "first_name": "Old",
#             "last_name": "Name",
#             "email": "old@example.com",
#         },
#         None,
#     )

#     assert event.table_name == "customers"
#     assert event.operation == "DELETE"
#     assert event.primary_key == {"customer_id": 1}
#     assert event.after is None
#     assert event.before["customer_id"] == 1

import pytest

from src.cdc.events import create_event, generate_event_id


def test_create_insert_event():
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

    assert event.table_name == "customers"
    assert event.operation == "INSERT"
    assert event.primary_key == {"customer_id": 4}
    assert event.before is None
    assert event.after["customer_id"] == 4
    assert event.after["email"] == "test@example.com"
    assert event.event_id
    assert event.captured_at


def test_create_update_event():
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

    assert event.table_name == "customers"
    assert event.operation == "UPDATE"
    assert event.primary_key == {"customer_id": 1}
    assert event.before["email"] == "old@example.com"
    assert event.after["email"] == "new@example.com"


def test_create_delete_event():
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

    assert event.table_name == "customers"
    assert event.operation == "DELETE"
    assert event.primary_key == {"customer_id": 1}
    assert event.after is None
    assert event.before["customer_id"] == 1


def test_same_change_produces_same_event_id():
    event_1 = create_event(
        "customers",
        "INSERT",
        {"customer_id": 10},
        None,
        {
            "customer_id": 10,
            "first_name": "Test",
            "last_name": "User",
            "email": "test10@example.com",
        },
    )

    event_2 = create_event(
        "customers",
        "INSERT",
        {"customer_id": 10},
        None,
        {
            "customer_id": 10,
            "first_name": "Test",
            "last_name": "User",
            "email": "test10@example.com",
        },
    )

    assert event_1.event_id == event_2.event_id


def test_different_changes_produce_different_event_ids():
    event_1 = create_event(
        "customers",
        "INSERT",
        {"customer_id": 10},
        None,
        {
            "customer_id": 10,
            "first_name": "Test",
            "last_name": "User",
            "email": "test10@example.com",
        },
    )

    event_2 = create_event(
        "customers",
        "INSERT",
        {"customer_id": 11},
        None,
        {
            "customer_id": 11,
            "first_name": "Test",
            "last_name": "User",
            "email": "test11@example.com",
        },
    )

    assert event_1.event_id != event_2.event_id


def test_invalid_operation_is_rejected():
    with pytest.raises(ValueError):
        create_event(
            "customers",
            "INVALID",
            {"customer_id": 1},
            None,
            {"customer_id": 1},
        )


def test_insert_requires_after_state():
    with pytest.raises(ValueError):
        create_event(
            "customers",
            "INSERT",
            {"customer_id": 1},
            None,
            None,
        )


def test_delete_requires_before_state():
    with pytest.raises(ValueError):
        create_event(
            "customers",
            "DELETE",
            {"customer_id": 1},
            None,
            None,
        )


def test_update_requires_before_and_after_state():
    with pytest.raises(ValueError):
        create_event(
            "customers",
            "UPDATE",
            {"customer_id": 1},
            None,
            {"customer_id": 1},
        )


def test_empty_primary_key_is_rejected():
    with pytest.raises(ValueError):
        create_event(
            "customers",
            "INSERT",
            {},
            None,
            {"customer_id": 1},
        )


def test_empty_table_name_is_rejected():
    with pytest.raises(ValueError):
        create_event(
            "",
            "INSERT",
            {"customer_id": 1},
            None,
            {"customer_id": 1},
        )


def test_event_json_is_deterministic():
    event = create_event(
        "customers",
        "INSERT",
        {"customer_id": 20},
        None,
        {
            "customer_id": 20,
            "first_name": "JSON",
            "last_name": "Test",
            "email": "json@example.com",
        },
    )

    json_1 = event.to_json()
    json_2 = event.to_json()

    assert json_1 == json_2