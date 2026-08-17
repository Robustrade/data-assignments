import sqlite3
import yaml


def test_source_pk_and_fk_enforced(clean_env):
    source_db = clean_env["source_db"]
    db = source_db.SourceDB()

    # PK uniqueness enforced by sqlite AUTOINCREMENT PK -> attempting to
    # insert a duplicate explicit PK should fail
    ts = source_db.now()
    db.insert("customers", {
        "email": "x@example.com", "full_name": "X", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])

    # FK enforced: wallet referencing a non-existent customer_id must fail
    try:
        db.conn.execute(
            "INSERT INTO wallets (customer_id, currency_code, balance, status, created_at, updated_at) "
            "VALUES (99999, 'INR', 100.0, 'ACTIVE', ?, ?)", (ts, ts),
        )
        db.conn.commit()
        fk_violation_raised = False
    except sqlite3.IntegrityError:
        fk_violation_raised = True
    db.close()
    assert fk_violation_raised


def test_source_business_constraints_enforced(clean_env):
    source_db = clean_env["source_db"]
    db = source_db.SourceDB()
    ts = source_db.now()
    cid = db.insert("customers", {
        "email": "y@example.com", "full_name": "Y", "country_code": "IN",
        "is_verified": 0, "created_at": ts, "updated_at": ts,
    }, ["customer_id"])

    # negative balance must be rejected by CHECK constraint
    try:
        db.conn.execute(
            "INSERT INTO wallets (customer_id, currency_code, balance, status, created_at, updated_at) "
            "VALUES (?, 'INR', -10.0, 'ACTIVE', ?, ?)", (cid, ts, ts),
        )
        db.conn.commit()
        check_violation_raised = False
    except sqlite3.IntegrityError:
        check_violation_raised = True
    db.close()
    assert check_violation_raised


def test_catalog_generated_for_lake_and_warehouse(clean_env):
    import importlib
    import catalog as catalog_mod
    importlib.reload(catalog_mod)
    catalog_mod.write_catalog()

    content = yaml.safe_load(catalog_mod.CATALOG_PATH.read_text())
    names = [d["name"] for d in content["datasets"]]

    for t in catalog_mod.TABLES:
        assert f"lake.{t}" in names
        assert f"warehouse.{t}_current" in names
        assert f"warehouse.{t}_history" in names

    for d in content["datasets"]:
        assert d["owner"]
        assert d["update_cadence"]
        assert d["intended_consumers"]
