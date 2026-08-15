from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.cdc.events import CDCEvent
from src.warehouse.writer import WarehouseWriter


class HistoricalRecovery:
    """
    Rebuild a historical warehouse state by replaying
    CDC events stored in the lake.

    Events are replayed in the same order in which they
    were appended to the lake JSONL file.
    """

    def __init__(
        self,
        lake_path: str | Path,
        warehouse_path: str | Path,
    ):
        self.lake_path = Path(lake_path)
        self.warehouse_path = Path(warehouse_path)

    def _event_file(self, table_name: str) -> Path:
        return self.lake_path / "{}.jsonl".format(table_name)

    def _read_events(
        self,
        table_name: str,
    ) -> list[CDCEvent]:
        file_path = self._event_file(table_name)

        if not file_path.exists():
            return []

        events = []

        with file_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                data = json.loads(line)

                events.append(
                    CDCEvent(
                        event_id=data["event_id"],
                        table_name=data["table_name"],
                        operation=data["operation"],
                        primary_key=data["primary_key"],
                        before=data.get("before"),
                        after=data.get("after"),
                        captured_at=data["captured_at"],
                    )
                )

        return events

    def replay(
        self,
        table_name: str,
        until: Optional[str] = None,
    ) -> WarehouseWriter:
        """
        Replay CDC events for a table.

        If `until` is supplied, only events captured at or
        before that timestamp are applied.

        Returns the rebuilt WarehouseWriter.
        """

        events = self._read_events(table_name)

        warehouse = WarehouseWriter(
            self.warehouse_path
        )

        cutoff = None

        if until is not None:
            cutoff = datetime.fromisoformat(until)

        for event in events:
            if cutoff is not None:
                event_time = datetime.fromisoformat(
                    event.captured_at
                )

                if event_time > cutoff:
                    continue

            warehouse.apply(event)

        return warehouse