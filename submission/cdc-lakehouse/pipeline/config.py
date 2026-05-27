"""Paths and environment configuration."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKE_DIR = Path(os.environ.get("LAKE_DIR", ROOT / "lake"))
WAREHOUSE_PATH = Path(
    os.environ.get("WAREHOUSE_PATH", ROOT / "warehouse" / "warehouse.duckdb")
)
STATE_DIR = Path(os.environ.get("STATE_DIR", ROOT / "state"))
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"
CONTRACTS_DIR = ROOT / "contracts"
SCHEMA_CONTRACT_PATH = CONTRACTS_DIR / "schema_contract.json"
SCHEMA_CONTRACT_SPEC_PATH = CONTRACTS_DIR / "schema_contract_spec.json"
SCHEMA_FINGERPRINT_PATH = CONTRACTS_DIR / "schema_fingerprint.json"
FAILED_SCHEMA_PATH = ROOT / "ingest" / "FAILED_SCHEMA_CHANGE.md"

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "cdc_user"),
    "password": os.environ.get("MYSQL_PASSWORD", "cdc_pass"),
    "database": os.environ.get("MYSQL_DATABASE", "cdc_source"),
}
