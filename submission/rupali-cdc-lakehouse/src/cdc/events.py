from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import hashlib
import json


VALID_OPERATIONS = {"INSERT", "UPDATE", "DELETE"}


@dataclass(frozen=True)
class CDCEvent:
    """
    Canonical representation of one source-system change.
    """

    event_id: str
    table_name: str
    operation: str
    primary_key: Dict[str, Any]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    captured_at: str

    def __post_init__(self) -> None:
        if self.operation not in VALID_OPERATIONS:
            raise ValueError(
                "Invalid CDC operation: {}".format(self.operation)
            )

        if not self.table_name:
            raise ValueError("table_name cannot be empty")

        if not self.primary_key:
            raise ValueError("primary_key cannot be empty")

        if self.operation == "INSERT" and self.after is None:
            raise ValueError("INSERT event must contain after state")

        if self.operation == "DELETE" and self.before is None:
            raise ValueError("DELETE event must contain before state")

        if self.operation == "UPDATE":
            if self.before is None or self.after is None:
                raise ValueError(
                    "UPDATE event must contain both before and after state"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "table_name": self.table_name,
            "operation": self.operation,
            "primary_key": self.primary_key,
            "before": self.before,
            "after": self.after,
            "captured_at": self.captured_at,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        )


def generate_event_id(
    table_name: str,
    operation: str,
    primary_key: Dict[str, Any],
    before: Optional[Dict[str, Any]],
    after: Optional[Dict[str, Any]],
) -> str:
    """
    Generate a deterministic event ID.

    The same source change produces the same event_id.
    This allows the lake writer to safely ignore duplicate/replayed events.
    """

    payload = {
        "table_name": table_name,
        "operation": operation,
        "primary_key": primary_key,
        "before": before,
        "after": after,
    }

    canonical_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical_payload.encode("utf-8")
    ).hexdigest()


def create_event(
    table_name: str,
    operation: str,
    primary_key: Dict[str, Any],
    before: Optional[Dict[str, Any]],
    after: Optional[Dict[str, Any]],
) -> CDCEvent:
    """
    Create a validated CDC event.
    """

    event_id = generate_event_id(
        table_name=table_name,
        operation=operation,
        primary_key=primary_key,
        before=before,
        after=after,
    )

    captured_at = datetime.now(timezone.utc).isoformat()

    return CDCEvent(
        event_id=event_id,
        table_name=table_name,
        operation=operation,
        primary_key=primary_key,
        before=before,
        after=after,
        captured_at=captured_at,
    )