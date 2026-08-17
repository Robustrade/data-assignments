import json
import duckdb


def _pipeline_run(source_db, cdc_extractor, warehouse_loader):
    source_db.seed()
    cdc_extractor.extract()
    return warehouse_loader.apply_events()


def test_latest_snapshot_matches_source(clean_env):
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]
    warehouse_loader = clean_env["warehouse_loader"]

    _pipeline_run(source_db, cdc_extractor, warehouse_loader)

    src = __import__("sqlite3").connect(source_db.DB_PATH)
    src_wallets = {r[0]: r[1] for r in src.execute("SELECT wallet_id, balance FROM wallets")}
    src.close()

    con = duckdb.connect(str(warehouse_loader.WAREHOUSE_PATH), read_only=True)
    rows = con.execute("SELECT pk_key, row_data FROM wallets_current").fetchall()
    con.close()
    wh_wallets = {int(pk): json.loads(row)["balance"] for pk, row in rows}

    assert wh_wallets == src_wallets


def test_validation_parity_all_pass_on_clean_data(clean_env):
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]
    warehouse_loader = clean_env["warehouse_loader"]
    validations = clean_env["validations"]

    _pipeline_run(source_db, cdc_extractor, warehouse_loader)

    report = validations.run_all(db_path=warehouse_loader.WAREHOUSE_PATH)
    failed = {k: v for k, v in report.items() if v}
    assert failed == {}, f"Unexpected validation failures: {failed}"


def test_validation_parity_catches_line_item_mismatch(clean_env):
    """RED: a transaction whose line items don't sum to its amount must be
    flagged by validation parity, mirroring the source business rule."""
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]
    warehouse_loader = clean_env["warehouse_loader"]
    validations = clean_env["validations"]

    db = source_db.SourceDB()
    ts = source_db.now()
    cid = db.insert("customers", {
        "email": "m@example.com", "full_name": "M", "country_code": "IN",
        "is_verified": 1, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])
    wid = db.insert("wallets", {
        "customer_id": cid, "currency_code": "INR", "balance": 100.0,
        "status": "ACTIVE", "created_at": ts, "updated_at": ts,
    }, ["wallet_id"])
    tid = db.insert("transactions", {
        "source_wallet_id": wid, "destination_wallet_id": None, "type": "WITHDRAWAL",
        "status": "COMPLETED", "amount": 100.0, "currency_code": "INR",
        "initiated_at": ts, "settled_at": ts, "failure_reason": None,
    }, ["transaction_id"])
    # deliberately mismatched line item (50 != 100)
    db.insert("transaction_line_items", {
        "transaction_id": tid, "line_number": 1, "description": "Partial",
        "amount": 50.0, "fee_flag": 0,
    }, ["transaction_id", "line_number"])
    db.close()

    cdc_extractor.extract()
    warehouse_loader.apply_events()

    report = validations.run_all(db_path=warehouse_loader.WAREHOUSE_PATH)
    assert report["check_line_items_sum_matches_transaction"] != []


def test_historical_reconstruction_time_travel(clean_env):
    """A wallet's balance before an update must be reconstructable via
    warehouse_loader.reconstruct_at using the SCD2 history table."""
    source_db = clean_env["source_db"]
    cdc_extractor = clean_env["cdc_extractor"]
    warehouse_loader = clean_env["warehouse_loader"]

    db = source_db.SourceDB()
    ts0 = source_db.now()
    cid = db.insert("customers", {
        "email": "t@example.com", "full_name": "T", "country_code": "IN",
        "is_verified": 1, "created_at": ts0, "updated_at": ts0,
    }, ["customer_id"])
    wid = db.insert("wallets", {
        "customer_id": cid, "currency_code": "INR", "balance": 1000.0,
        "status": "ACTIVE", "created_at": ts0, "updated_at": ts0,
    }, ["wallet_id"])
    db.close()

    cdc_extractor.extract()
    warehouse_loader.apply_events()

    import time
    time.sleep(0.01)
    midpoint_ts = source_db.now()
    time.sleep(0.01)

    db = source_db.SourceDB()
    db.update("wallets", {"wallet_id": wid}, {"balance": 400.0, "updated_at": source_db.now()})
    db.close()

    cdc_extractor.extract()
    warehouse_loader.apply_events()

    as_of_old = warehouse_loader.reconstruct_at("wallets", str(wid), midpoint_ts)
    assert as_of_old["balance"] == 1000.0

    con = duckdb.connect(str(warehouse_loader.WAREHOUSE_PATH), read_only=True)
    current_row = json.loads(
        con.execute("SELECT row_data FROM wallets_current WHERE pk_key = ?", [str(wid)]).fetchone()[0]
    )
    con.close()
    assert current_row["balance"] == 400.0
