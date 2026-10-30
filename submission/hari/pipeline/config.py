"""Configuration: ``config.yml`` with environment-variable overrides (OPERATIONS.md §1)."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config.yml"

ENV_VARS = {
    "source_db": "CDC_SOURCE_DB",
    "platform_db": "CDC_PLATFORM_DB",
    "contract_path": "CDC_CONTRACT_PATH",
    "batch_size": "CDC_BATCH_SIZE",
    "poll_min_s": "CDC_POLL_MIN_S",
    "poll_max_s": "CDC_POLL_MAX_S",
    "freshness_warn_s": "CDC_FRESHNESS_WARN_S",
    "freshness_crit_s": "CDC_FRESHNESS_CRIT_S",
}


@dataclass(frozen=True)
class Config:
    source_db: Path
    platform_db: Path
    contract_path: Path
    batch_size: int = 500
    poll_min_s: float = 0.2
    poll_max_s: float = 5.0
    freshness_warn_s: float = 60.0
    freshness_crit_s: float = 300.0

    def with_overrides(self, **kwargs) -> Config:
        return replace(self, **kwargs)


def load_config(path: str | Path | None = None, env=None, **overrides) -> Config:
    """Load config: file values, then environment variables, then explicit overrides."""
    env = os.environ if env is None else env
    path = Path(path) if path else DEFAULT_CONFIG
    values: dict = {}
    if path.exists():
        with open(path, encoding="utf-8") as handle:
            values.update(yaml.safe_load(handle) or {})
    base = path.parent
    for key, var in ENV_VARS.items():
        if var in env:
            values[key] = env[var]
    values.update({k: v for k, v in overrides.items() if v is not None})
    for key in ("source_db", "platform_db", "contract_path"):
        resolved = Path(values[key])
        values[key] = resolved if resolved.is_absolute() else base / resolved
    return Config(
        source_db=values["source_db"],
        platform_db=values["platform_db"],
        contract_path=values["contract_path"],
        batch_size=int(values["batch_size"]),
        poll_min_s=float(values["poll_min_s"]),
        poll_max_s=float(values["poll_max_s"]),
        freshness_warn_s=float(values["freshness_warn_s"]),
        freshness_crit_s=float(values["freshness_crit_s"]),
    )
