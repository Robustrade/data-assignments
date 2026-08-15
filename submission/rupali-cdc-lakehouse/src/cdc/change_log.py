from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


class ChangeLog:
    """
    Durable append-only change log for CDC events.

    Each unique event receives a monotonically increasing
    sequence_number.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def append(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Append a CDC event unless the event_id already exists.

        Duplicate events return the original stored event instead
        of creating a second change-log entry.
        """

        events = self.read_all()

        # Check whether this event was already recorded.
        for existing_event in events:
            if existing_event["event_id"] == event["event_id"]:
                return existing_event

        sequence_number = len(events) + 1

        event_with_sequence = dict(event)

        event_with_sequence["sequence_number"] = sequence_number

        with self.path.open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                json.dumps(
                    event_with_sequence,
                    sort_keys=True,
                )
                + "\n"
            )

        return event_with_sequence

    def read_all(self) -> List[Dict[str, Any]]:
        """
        Read all CDC events from the change log.
        """

        if not self.path.exists():
            return []

        events: List[Dict[str, Any]] = []

        with self.path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                line = line.strip()

                if line:
                    events.append(
                        json.loads(line)
                    )

        return events