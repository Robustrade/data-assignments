"""
Tests — lake immutability and completeness.

Covers:
  - every CDC operation lands in lake_cdc_events
  - row count is monotonically non-decreasing
  - duplicate sequence is rejected by PK (or silently deduped)
  - before / after JSON is round-trippable
  - replay_records returns events in sequence order
  - lake retains every operation for the same PK
"""

from __future__ import annotations

import json

from pipeline.lake import append_to_lake, replay_records


def test_seed_writes_all_events_to_lake(seeded):
    conn, capture = seeded
    n_lake = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    assert n_lake == capture.latest_sequence


def test_lake_per_table_event_counts(seeded):
    conn, _ = seeded
    rows = conn.execute(
        """
        SELECT table_name, COUNT(*)
        FROM   lake_cdc_events
        GROUP BY table_name
        """
    ).fetchall()
    counts = dict(rows)
    # 3 customers + 3 payment methods + 4 wallets + 3 transfers
    # + 5 transfer_events + 5 ledger_entries = 23 inserts
    assert counts == {
        "customers": 3,
        "payment_methods": 3,
        "wallets": 4,
        "transfers": 3,
        "transfer_events": 5,
        "ledger_entries": 5,
    }


def test_lake_row_count_only_grows(seeded):
    conn, capture = seeded
    before = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]

    capture.update(
        "customers", "c_alice",
        before={"customer_id": "c_alice", "status": "active"},
        after ={"customer_id": "c_alice", "status": "suspended"},
    )
    new_records = capture.records_since(before)
    append_to_lake(conn, new_records)

    after = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    assert after > before


def test_lake_retains_full_pk_history(seeded):
    conn, capture = seeded
    capture.update(
        "customers", "c_alice",
        before={"customer_id": "c_alice", "status": "active"},
        after ={"customer_id": "c_alice", "status": "suspended"},
    )
    capture.delete(
        "customers", "c_alice", before={"customer_id": "c_alice"}
    )
    append_to_lake(conn, capture.records_since(0))

    ops = [
        r[0]
        for r in conn.execute(
            """
            SELECT operation FROM lake_cdc_events
            WHERE  table_name = 'customers' AND primary_key = 'c_alice'
            ORDER BY sequence
            """
        ).fetchall()
    ]
    assert ops == ["insert", "update", "delete"]


def test_duplicate_sequence_is_deduped(seeded):
    conn, capture = seeded
    before = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]

    # Re-append the same records — the lake should silently dedup.
    append_to_lake(conn, capture.records_since(0))
    after = conn.execute(
        "SELECT COUNT(*) FROM lake_cdc_events"
    ).fetchone()[0]
    assert before == after


def test_replay_records_returns_in_sequence_order(seeded):
    conn, _ = seeded
    rows = replay_records(conn)
    sequences = [r["sequence"] for r in rows]
    assert sequences == sorted(sequences)
    assert sequences[0] == 1


def test_replay_records_filters_by_table(seeded):
    conn, _ = seeded
    customers = replay_records(conn, table="customers")
    assert all(r["table"] == "customers" for r in customers)


def test_replay_records_filters_by_until_sequence(seeded):
    conn, _ = seeded
    until = 5
    rows = replay_records(conn, until_sequence=until)
    assert all(r["sequence"] <= until for r in rows)


def test_lake_payload_is_valid_json_for_inserts(seeded):
    conn, _ = seeded
    row = conn.execute(
        "SELECT after_image FROM lake_cdc_events "
        "WHERE table_name = 'customers' AND operation = 'insert' LIMIT 1"
    ).fetchone()
    assert row is not None
    parsed = json.loads(row[0])
    assert "customer_id" in parsed
