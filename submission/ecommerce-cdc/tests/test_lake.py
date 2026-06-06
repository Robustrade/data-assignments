"""Lake tests: full change retention + idempotent append."""

from ingestion.cdc import CDCCapture
from ingestion.pipeline import run_ingestion
from lake.lake import append_to_lake, read_events


def test_lake_retains_every_change(ingested):
    # 12 seed events: 9 inserts + 2 updates + 1 delete.
    total = ingested.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert total == 12

    ops = ingested.execute(
        "SELECT operation, COUNT(*) FROM lake_cdc_events GROUP BY operation"
    ).fetchall()
    counts = dict(ops)
    assert counts == {"insert": 9, "update": 2, "delete": 1}


def test_lake_append_is_idempotent(conn):
    cap = CDCCapture()
    cap.insert("customers", "c1", {"customer_id": "c1"})
    records = cap.records_since(0)

    assert append_to_lake(conn, records) == 1
    assert append_to_lake(conn, records) == 0  # duplicate replay is a no-op

    total = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert total == 1


def test_rerunning_ingestion_does_not_duplicate(conn):
    run_ingestion(conn, _seed())
    # A second run with the same capture re-reads from the advanced checkpoint -> no-op.
    cap = _seed()
    result = run_ingestion(conn, cap)
    assert result.applied == 0
    total = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    assert total == 12


def test_read_events_up_to_sequence(ingested):
    events = read_events(ingested, up_to_sequence=6)
    assert all(e["sequence"] <= 6 for e in events)
    assert len(events) == 6


def _seed():
    from ingestion.seed import seed_capture

    return seed_capture()
