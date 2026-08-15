import json
from pathlib import Path
from typing import Callable, Dict, Any


class CDCProcessor:
    """
    Processes CDC events from the change log after the last
    successfully processed checkpoint sequence number.
    """

    def __init__(
        self,
        change_log,
        checkpoint_path,
        handler: Callable[[Dict[str, Any]], None],
    ):
        self.change_log = change_log
        self.checkpoint_path = Path(checkpoint_path)
        self.handler = handler

    def _read_checkpoint(self) -> int:
        if not self.checkpoint_path.exists():
            return 0

        with self.checkpoint_path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        return int(data.get("sequence_number", 0))

    def _write_checkpoint(self, sequence_number: int) -> None:
        self.checkpoint_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with self.checkpoint_path.open("w", encoding="utf-8") as file:
            json.dump(
                {"sequence_number": sequence_number},
                file,
                indent=2,
            )

    def process(self) -> None:
        last_sequence = self._read_checkpoint()

        events = self.change_log.read_all()

        for event in events:
            sequence_number = int(event["sequence_number"])

            if sequence_number <= last_sequence:
                continue

            self.handler(event)

            self._write_checkpoint(sequence_number)

            last_sequence = sequence_number