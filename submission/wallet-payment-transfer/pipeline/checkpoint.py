"""
Checkpoint store — persists the last sequence successfully applied.

Why this matters
----------------
After an ingestion restart, the pipeline must answer two questions:
  1. Which CDC events have already been written to the lake?
  2. Which CDC events have already been merged into the warehouse?

Both answers are durable in their respective layers (lake by design,
warehouse by `_cdc_seq` per row), but having a *single point of truth*
for the latest applied sequence simplifies operations: a small
checkpoint table the orchestrator reads at startup.

Production analogue
-------------------
A Kafka consumer group offset committed atomically with the warehouse
batch, or a row in a small `pipeline_state` table written inside the
same transaction as the warehouse merge.
"""

from __future__ import annotations

import duckdb


def create_checkpoint_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
            stage      VARCHAR  PRIMARY KEY,
            sequence   INTEGER  NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)


def get_checkpoint(conn: duckdb.DuckDBPyConnection, stage: str) -> int:
    row = conn.execute(
        "SELECT sequence FROM pipeline_checkpoints WHERE stage = ?",
        [stage],
    ).fetchone()
    return int(row[0]) if row else 0


def set_checkpoint(
    conn: duckdb.DuckDBPyConnection,
    stage: str,
    sequence: int,
) -> None:
    """
    Upsert the checkpoint for a stage, but never move it backwards.
    A backwards-moving checkpoint would silently widen the replay window
    and could double-apply events that the warehouse would otherwise
    have rejected — better to make the bug loud.
    """
    current = get_checkpoint(conn, stage)
    if sequence < current:
        raise ValueError(
            f"refusing to move checkpoint {stage!r} backwards: "
            f"current={current}, requested={sequence}"
        )
    conn.execute(
        """
        INSERT INTO pipeline_checkpoints (stage, sequence, updated_at)
        VALUES (?, ?, current_timestamp)
        ON CONFLICT (stage) DO UPDATE
            SET sequence   = EXCLUDED.sequence,
                updated_at = EXCLUDED.updated_at
        """,
        [stage, sequence],
    )
