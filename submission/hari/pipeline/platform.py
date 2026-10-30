"""DuckDB platform: connection, schemas and numbered migrations."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

MIGRATIONS: list[tuple[int, str, list[str]]] = [
    (
        1,
        "schemas_and_cdc_state",
        [
            "CREATE SCHEMA IF NOT EXISTS lake",
            "CREATE SCHEMA IF NOT EXISTS warehouse",
            "CREATE SCHEMA IF NOT EXISTS cdc",
            "CREATE SCHEMA IF NOT EXISTS quality",
            (
                "CREATE TABLE IF NOT EXISTS cdc.checkpoint ("
                "id INTEGER, last_seq BIGINT NOT NULL, updated_at TIMESTAMP)"
            ),
            (
                "CREATE TABLE IF NOT EXISTS cdc.pipeline_status ("
                "id INTEGER, status VARCHAR NOT NULL, reason VARCHAR, since TIMESTAMP, "
                "last_batch_at TIMESTAMP, last_batch_events INTEGER, "
                "lag_events BIGINT, freshness_s DOUBLE)"
            ),
            "CREATE SEQUENCE IF NOT EXISTS cdc.alert_seq",
            (
                "CREATE TABLE IF NOT EXISTS cdc.alerts ("
                "alert_id BIGINT DEFAULT nextval('cdc.alert_seq'), ts TIMESTAMP NOT NULL, "
                "level VARCHAR NOT NULL, status VARCHAR, reason VARCHAR NOT NULL, details VARCHAR)"
            ),
            (
                "CREATE TABLE IF NOT EXISTS cdc.schema_registry ("
                "version INTEGER PRIMARY KEY, contract_json VARCHAR NOT NULL, "
                "diff_json VARCHAR, approved_at TIMESTAMP NOT NULL)"
            ),
            (
                "CREATE TABLE IF NOT EXISTS cdc.pipeline_lock ("
                "id INTEGER, owner VARCHAR NOT NULL, heartbeat_at TIMESTAMP NOT NULL)"
            ),
            (
                "CREATE TABLE IF NOT EXISTS cdc.quarantine_overrides ("
                "seq BIGINT, actor VARCHAR, reason VARCHAR, created_at TIMESTAMP, "
                "applied_batch VARCHAR)"
            ),
        ],
    ),
    (
        2,
        "quality",
        [
            (
                "CREATE TABLE IF NOT EXISTS quality.check_results ("
                "batch_id VARCHAR, rule_id VARCHAR, severity VARCHAR, status VARCHAR, "
                "violations INTEGER, checked_at TIMESTAMP)"
            ),
            "CREATE SEQUENCE IF NOT EXISTS quality.quarantine_seq",
            (
                "CREATE TABLE IF NOT EXISTS quality.quarantine ("
                "quarantine_id BIGINT DEFAULT nextval('quality.quarantine_seq'), "
                "seq BIGINT, table_name VARCHAR, pk VARCHAR, rule_id VARCHAR, reason VARCHAR, "
                "batch_id VARCHAR, quarantined_at TIMESTAMP)"
            ),
        ],
    ),
    (
        3,
        "lake",
        [
            (
                "CREATE TABLE IF NOT EXISTS lake.cdc_events ("
                "seq BIGINT PRIMARY KEY, txn_id VARCHAR, schema_version INTEGER, "
                "table_name VARCHAR NOT NULL, op VARCHAR NOT NULL, pk VARCHAR NOT NULL, "
                '"before" VARCHAR, "after" VARCHAR, changed_at TIMESTAMP NOT NULL, '
                "batch_id VARCHAR, ingested_at TIMESTAMP, contract_version INTEGER)"
            ),
        ],
    ),
]


def connect(path: str | Path) -> duckdb.DuckDBPyConnection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def migrate(conn: duckdb.DuckDBPyConnection, now: datetime) -> list[int]:
    """Apply numbered migrations that are not yet recorded. Returns the versions applied."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS cdc")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cdc.migrations ("
        "version INTEGER PRIMARY KEY, name VARCHAR NOT NULL, applied_at TIMESTAMP NOT NULL)"
    )
    done = {r[0] for r in conn.execute("SELECT version FROM cdc.migrations").fetchall()}
    applied = []
    stamp = now.astimezone(UTC).replace(tzinfo=None)
    for version, name, statements in MIGRATIONS:
        if version in done:
            continue
        conn.begin()
        try:
            for statement in statements:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO cdc.migrations VALUES (?, ?, ?)", [version, name, stamp]
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        applied.append(version)
    return applied
