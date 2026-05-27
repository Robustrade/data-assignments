"""CDC event model shared across lake, warehouse, and ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

VALID_OPS = frozenset({"I", "U", "D"})


@dataclass(frozen=True)
class CDCEvent:
    event_id: int
    source_table: str
    op: str
    pk_json: str
    row_json: str
    event_ts: datetime

    def __post_init__(self) -> None:
        if self.op not in VALID_OPS:
            raise ValueError(
                f"Invalid op {self.op!r}; expected one of {sorted(VALID_OPS)}"
            )

    @property
    def primary_key(self) -> dict[str, Any]:
        return json.loads(self.pk_json)

    @property
    def row(self) -> dict[str, Any]:
        return json.loads(self.row_json)

    @classmethod
    def from_mysql_row(cls, row: dict[str, Any]) -> CDCEvent:
        return cls(
            event_id=int(row["event_id"]),
            source_table=str(row["source_table"]),
            op=str(row["op"]),
            pk_json=_to_json_str(row["pk_json"]),
            row_json=_to_json_str(row["row_json"]),
            event_ts=row["event_ts"],
        )


def _to_json_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return json.dumps(value, default=str)
