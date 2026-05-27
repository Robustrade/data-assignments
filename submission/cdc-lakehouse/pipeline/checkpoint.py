"""Checkpoint persistence for CDC ingestion."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.config import CHECKPOINT_PATH, STATE_DIR


def load_checkpoint(path: Path | None = None) -> int:
    cp_path = path or CHECKPOINT_PATH
    if not cp_path.exists():
        return 0
    data = json.loads(cp_path.read_text(encoding="utf-8"))
    return int(data.get("last_event_id", 0))


def save_checkpoint(last_event_id: int, path: Path | None = None) -> None:
    cp_path = path or CHECKPOINT_PATH
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    cp_path.write_text(
        json.dumps({"last_event_id": last_event_id}, indent=2), encoding="utf-8"
    )
