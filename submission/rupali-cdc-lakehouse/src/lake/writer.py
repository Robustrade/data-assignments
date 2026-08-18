from __future__ import annotations

import json
from pathlib import Path
from typing import List

from src.cdc.events import CDCEvent


class LakeWriter:
    """
    Append-only JSONL writer for CDC events.

    Each table gets its own JSONL file:

        lake/
            customers.jsonl
            products.jsonl
            orders.jsonl
            order_items.jsonl
            payments.jsonl

    The complete CDC event is preserved so the lake can be used
    as the historical source of change data.
    """

    def __init__(self, lake_path: str | Path):
        self.lake_path = Path(lake_path)
        self.lake_path.mkdir(parents=True, exist_ok=True)

    def _event_file(self, table_name: str) -> Path:
        return self.lake_path / "{}.jsonl".format(table_name)

    def _existing_event_ids(self, file_path: Path) -> set[str]:
        """
        Read existing event IDs so replaying the same event
        does not create a duplicate.
        """

        if not file_path.exists():
            return set()

        event_ids = set()

        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                record = json.loads(line)

                event_id = record.get("event_id")

                if event_id:
                    event_ids.add(event_id)

        return event_ids

    def write(self, event: CDCEvent) -> bool:
        """
        Append one CDC event to the lake.

        Returns:
            True  -> event was written
            False -> event already existed and was skipped
        """

        file_path = self._event_file(event.table_name)

        existing_event_ids = self._existing_event_ids(file_path)

        if event.event_id in existing_event_ids:
            return False

        with file_path.open("a", encoding="utf-8") as file:
            file.write(event.to_json())
            file.write("\n")

        return True

    def read_events(self, table_name: str) -> List[dict]:
        """
        Read all historical CDC events for a table.
        """

        file_path = self._event_file(table_name)

        if not file_path.exists():
            return []

        events = []

        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if line:
                    events.append(json.loads(line))

        return events