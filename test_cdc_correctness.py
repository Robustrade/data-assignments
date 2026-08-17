import json
from pathlib import Path


def _lake_events(base, table):
    events = []
    lake_dir = base / "lake" / table
    if not lake_dir.exists():
        return events
    for f in lake_dir.glob("dt=*/changes.jsonl"):
        for line in f.read_text().splitlines():
            events.append(json.loads(line))
    return sorted(events, key=lambda e: e["log_id"])


def test_insert_capture(clean_env):
    """RED->BLUE: an insert on the source must produce exactly one lake event."""
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]

    db = source_db.SourceDB()
    ts = source_db.now()
    cid = db.insert("customers", {
        "email": "test@example.com", "full_name": "Test User", "country_code": "IN",
        "is_verified": 1, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    db.close()

    summary = cdc_extractor.extract()
    assert summary["events_written"] == 1

    events = _lake_events(cdc_extractor.BASE, "customers")
    assert len(events) == 1
    assert events[0]["op"] == "INSERT"
    assert events[0]["row"]["email"] == "test@example.com"


def test_update_capture(clean_env):
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]

    db = source_db.SourceDB()
    ts = source_db.now()
    cid = db.insert("customers", {
        "email": "u@example.com", "full_name": "U", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    db.update("customers", {"customer_id": cid}, {"is_verified": 1, "updated_at": source_db.now()})
    db.close()

    cdc_extractor.extract()
    events = _lake_events(cdc_extractor.BASE, "customers")
    ops = [e["op"] for e in events]
    assert ops == ["INSERT", "UPDATE"]
    assert events[1]["row"]["is_verified"] == 1


def test_delete_capture(clean_env):
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]

    db = source_db.SourceDB()
    ts = source_db.now()
    cid = db.insert("customers", {
        "email": "d@example.com", "full_name": "D", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    db.delete("customers", {"customer_id": cid})
    db.close()

    cdc_extractor.extract()
    events = _lake_events(cdc_extractor.BASE, "customers")
    ops = [e["op"] for e in events]
    assert ops == ["INSERT", "DELETE"]


def test_replay_does_not_duplicate_in_warehouse(clean_env):
    """Re-running extraction+load against unmoved checkpoint must not double-count
    events in the warehouse (dedup by log_id)."""
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]
    warehouse_loader = clean_env["warehouse_loader"]

    db = source_db.SourceDB()
    ts = source_db.now()
    db.insert("customers", {
        "email": "r@example.com", "full_name": "R", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    db.close()

    cdc_extractor.extract()
    first_load = warehouse_loader.apply_events()
    assert first_load["events_applied"] == 1

    # Simulate replay: force-reset checkpoint backward and re-extract the same
    # log range (duplicate delivery to the lake), then reload the warehouse.
    cdc_extractor.extract(reset_checkpoint_to=0)
    second_load = warehouse_loader.apply_events()
    assert second_load["events_applied"] == 0  # deduped by log_id, nothing new applied

    import duckdb
    con = duckdb.connect(str(warehouse_loader.WAREHOUSE_PATH), read_only=True)
    count = con.execute("SELECT COUNT(*) FROM customers_current").fetchone()[0]
    con.close()
    assert count == 1  # not duplicated


def test_restart_from_checkpoint_after_partial_failure(clean_env):
    """Extraction resumes from the last saved checkpoint, not from scratch,
    simulating a crash-and-restart."""
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]

    db = source_db.SourceDB()
    ts = source_db.now()
    db.insert("customers", {
        "email": "a@example.com", "full_name": "A", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    db.close()
    s1 = cdc_extractor.extract()
    assert s1["events_written"] == 1

    db = source_db.SourceDB()
    db.insert("customers", {
        "email": "b@example.com", "full_name": "B", "country_code": "IN",
        "is_verified": 0, "created_at": source_db.now(), "updated_at": source_db.now(),
    }, ["customer_id"])
    db.close()

    # second run only picks up the NEW event, proving checkpoint advanced correctly
    s2 = cdc_extractor.extract()
    assert s2["events_written"] == 1
    assert s2["checkpoint_before"] == s1["checkpoint_after"]
