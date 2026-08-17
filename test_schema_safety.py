import sqlite3
import pytest


def test_first_run_establishes_baseline_without_error(clean_env):
    source_db = clean_env["source_db"]
    schema_guard = clean_env["schema_guard"]

    db = source_db.SourceDB()
    schema_guard.check(db.conn)  # first run: no known-good yet, should not raise
    db.close()
    assert schema_guard.FINGERPRINT_PATH.exists()


def test_compatible_run_passes_silently(clean_env):
    source_db = clean_env["source_db"]
    schema_guard = clean_env["schema_guard"]

    db = source_db.SourceDB()
    schema_guard.check(db.conn)
    schema_guard.check(db.conn)  # unchanged schema, second call should also pass
    db.close()


def test_dropped_column_halts_ingestion(clean_env):
    """RED: dropping a tracked column must raise SchemaDriftError and must
    prevent cdc_extractor from writing anything to the lake."""
    source_db = clean_env["source_db"]
    schema_guard = clean_env["schema_guard"]
    cdc_extractor = clean_env["cdc_extractor"]

    db = source_db.SourceDB()
    schema_guard.check(db.conn)  # establish baseline
    db.conn.execute("ALTER TABLE customers DROP COLUMN country_code")
    db.conn.commit()
    db.close()

    with pytest.raises(schema_guard.SchemaDriftError):
        cdc_extractor.extract()

    # nothing should have been written to the lake
    lake_files = list((cdc_extractor.BASE / "lake").glob("**/*.jsonl"))
    assert lake_files == []


def test_dropped_column_emits_alert(clean_env):
    source_db = clean_env["source_db"]
    schema_guard = clean_env["schema_guard"]

    db = source_db.SourceDB()
    schema_guard.check(db.conn)
    db.conn.execute("ALTER TABLE wallets DROP COLUMN status")
    db.conn.commit()

    with pytest.raises(schema_guard.SchemaDriftError):
        schema_guard.check(db.conn)
    db.close()

    assert schema_guard.ALERT_LOG_PATH.exists()
    content = schema_guard.ALERT_LOG_PATH.read_text()
    assert "wallets.status" in content
    assert "SCHEMA_DRIFT_HALT" in content


def test_type_change_is_detected(clean_env):
    source_db = clean_env["source_db"]
    schema_guard = clean_env["schema_guard"]

    db = source_db.SourceDB()
    schema_guard.check(db.conn)

    # SQLite has no native ALTER COLUMN TYPE; simulate by rebuilding the
    # column with a different declared type to exercise the same detection path.
    db.conn.executescript("""
        ALTER TABLE transactions RENAME TO transactions_old;
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_wallet_id INTEGER,
            destination_wallet_id INTEGER,
            type TEXT NOT NULL,
            status TEXT NOT NULL,
            amount TEXT NOT NULL,  -- was REAL, now TEXT: incompatible type change
            currency_code TEXT NOT NULL,
            initiated_at TEXT NOT NULL,
            settled_at TEXT,
            failure_reason TEXT
        );
    """)
    db.conn.commit()

    with pytest.raises(schema_guard.SchemaDriftError) as exc_info:
        schema_guard.check(db.conn)
    assert "amount" in str(exc_info.value)
    db.close()
