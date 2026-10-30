"""SQLite connection and source database creation."""

from __future__ import annotations

import sqlite3

from pipeline.contract import Contract

from source import ddl


def connect(path: str) -> sqlite3.Connection:
    """Open the source database: foreign keys on, WAL, explicit transactions."""
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_source(conn: sqlite3.Connection, contract: Contract) -> None:
    """Create tables, constraints, indexes, guard triggers and CDC triggers."""
    for statement in ddl.schema_statements(contract):
        conn.execute(statement)
