"""
Checkpoint store.

Persists the last successfully applied CDC ``sequence`` so ingestion can restart
without losing or reprocessing changes. Backed by a DuckDB table so the checkpoint
shares the warehouse's durability guarantees.

Production analogue: a Kafka consumer group offset or a Postgres replication-slot
confirmed_flush_lsn.
"""

from __future__ import annotations

import duckdb

_CHECKPOINT_TABLE = "_ingest_checkpoint"


def create_checkpoint_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_CHECKPOINT_TABLE} (
            consumer  VARCHAR PRIMARY KEY,
            sequence  INTEGER NOT NULL
        )
        """
    )


def get_checkpoint(conn: duckdb.DuckDBPyConnection, consumer: str = "default") -> int:
    create_checkpoint_table(conn)
    row = conn.execute(
        f"SELECT sequence FROM {_CHECKPOINT_TABLE} WHERE consumer = ?",
        [consumer],
    ).fetchone()
    return int(row[0]) if row else 0


def set_checkpoint(
    conn: duckdb.DuckDBPyConnection, sequence: int, consumer: str = "default"
) -> None:
    create_checkpoint_table(conn)
    conn.execute(
        f"""
        INSERT INTO {_CHECKPOINT_TABLE} (consumer, sequence)
        VALUES (?, ?)
        ON CONFLICT (consumer) DO UPDATE SET sequence = excluded.sequence
        """,
        [consumer, sequence],
    )
