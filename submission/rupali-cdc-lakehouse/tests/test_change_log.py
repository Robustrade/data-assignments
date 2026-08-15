from src.cdc.change_log import ChangeLog


def test_change_log_assigns_sequence_numbers(tmp_path):
    log = ChangeLog(tmp_path / "change_log.jsonl")

    event1 = {
        "event_id": "event-1",
        "table_name": "customers",
        "operation": "INSERT",
        "record_id": {"customer_id": 1},
        "changed_data": {
            "customer_id": 1,
            "first_name": "Rupali",
        },
        "event_timestamp": "2026-08-15T10:00:00+00:00",
    }

    event2 = {
        "event_id": "event-2",
        "table_name": "customers",
        "operation": "UPDATE",
        "record_id": {"customer_id": 1},
        "changed_data": {
            "first_name": "Rupali Updated",
        },
        "event_timestamp": "2026-08-15T10:01:00+00:00",
    }

    log.append(event1)
    log.append(event2)

    events = log.read_all()

    assert len(events) == 2
    assert events[0]["sequence_number"] == 1
    assert events[1]["sequence_number"] == 2


def test_duplicate_event_is_not_appended_twice(tmp_path):
    log = ChangeLog(tmp_path / "change_log.jsonl")

    event = {
        "event_id": "event-1",
        "table_name": "customers",
        "operation": "INSERT",
        "record_id": {"customer_id": 1},
        "changed_data": {
            "customer_id": 1,
            "first_name": "Rupali",
        },
        "event_timestamp": "2026-08-15T10:00:00+00:00",
    }

    first = log.append(event)
    second = log.append(event)

    assert first["sequence_number"] == 1
    assert second["sequence_number"] == 1

    events = log.read_all()

    assert len(events) == 1