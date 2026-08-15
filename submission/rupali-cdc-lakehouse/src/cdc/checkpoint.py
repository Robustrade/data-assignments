from __future__ import annotations

import json
from pathlib import Path


class Checkpoint:
    """
    Durable checkpoint storing the latest successfully processed
    CDC sequence number.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def get(self) -> int:
        """
        Return the latest successfully processed sequence number.

        A new checkpoint starts at 0.
        """

        if not self.path.exists():
            return 0

        with self.path.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        return int(data.get("sequence_number", 0))

    def save(self, sequence_number: int) -> None:
        """
        Persist the latest successfully processed sequence number.
        """

        if sequence_number < 0:
            raise ValueError(
                "sequence_number cannot be negative"
            )

        data = {
            "sequence_number": sequence_number
        }

        with self.path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                data,
                file,
                indent=2,
            )