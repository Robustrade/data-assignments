from src.cdc.change_log import ChangeLog
from src.cdc.processor import CDCProcessor


def test_processor_processes_events_after_checkpoint(tmp_path):
    change_log = ChangeLog(tmp_path / "change_log.jsonl")
    checkpoint_file = tmp_path / "checkpoint.json"

    events = [
        {
            "event_id": "event-1",
            "table_name": "customers",
            "operation": "INSERT",
            "record_id": {"customer_id": 1},
            "changed_data": {
                "customer_id": 1,
                "first_name": "Rupali",
            },
            "event_timestamp": "2026-08-15T10:00:00+00:00",
        },
        {
            "event_id": "event-2",
            "table_name": "customers",
            "operation": "UPDATE",
            "record_id": {"customer_id": 1},
            "changed_data": {
                "first_name": "Rupali Updated",
            },
            "event_timestamp": "2026-08-15T10:01:00+00:00",
        },
        {
            "event_id": "event-3",
            "table_name": "customers",
            "operation": "DELETE",
            "record_id": {"customer_id": 1},
            "changed_data": {},
            "event_timestamp": "2026-08-15T10:02:00+00:00",
        },
    ]

    for event in events:
        change_log.append(event)

    processed = []

    def handler(event):
        processed.append(event)

    processor = CDCProcessor(
        change_log=change_log,
        checkpoint_path=checkpoint_file,
        handler=handler,
    )

    processor.process()

    assert len(processed) == 3
    assert processed[0]["event_id"] == "event-1"
    assert processed[1]["event_id"] == "event-2"
    assert processed[2]["event_id"] == "event-3"


def test_processor_does_not_reprocess_checkpointed_events(tmp_path):
    change_log = ChangeLog(tmp_path / "change_log.jsonl")
    checkpoint_file = tmp_path / "checkpoint.json"

    event1 = {
        "event_id": "event-1",
        "table_name": "customers",
        "operation": "INSERT",
        "record_id": {"customer_id": 1},
        "changed_data": {
            "customer_id": 1,
        },
        "event_timestamp": "2026-08-15T10:00:00+00:00",
    }

    event2 = {
        "event_id": "event-2",
        "table_name": "customers",
        "operation": "UPDATE",
        "record_id": {"customer_id": 1},
        "changed_data": {
            "first_name": "Updated",
        },
        "event_timestamp": "2026-08-15T10:01:00+00:00",
    }

    change_log.append(event1)
    change_log.append(event2)

    processed = []

    def handler(event):
        processed.append(event)

    processor = CDCProcessor(
        change_log=change_log,
        checkpoint_path=checkpoint_file,
        handler=handler,
    )

    processor.process()

    assert len(processed) == 2

    # Run again after checkpoint has reached sequence 2.
    processor.process()

    # Events must not be processed again.
    assert len(processed) == 2