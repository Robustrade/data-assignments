"""
test_pipeline.py — validation suite for the CDC lakehouse pipeline.

Covers the categories required by the assignment:
  1. Modeling/constraints — source-side business rules are enforced.
  2. CDC correctness — insert/update/delete capture, idempotent replay.
  3. Warehouse correctness — snapshot matches source, SCD2 history is sane.
  4. Schema-change safety — breaking changes are detected and halt ingestion.

Assumes: Postgres running (docker container `cdc-postgres`), schema +
warehouse_schema already applied, and seed.sql already loaded at least
once. Some tests insert/mutate their own throwaway rows (cleaned up via
rollback) rather than depending on exact seed state, so this suite is
safe to re-run.

Run with:
    pip3 install pytest psycopg2-binary
    pytest submission/megha/test_pipeline.py -v
"""

import json
import os
import subprocess
import sys

import psycopg2
import psycopg2.extras
import pytest

DB_CONFIG = dict(
    host="localhost", port=5432, dbname="wallet_db",
    user="postgres", password="postgres",
)

# The .py scripts (cdc_extract.py, warehouse_load.py, schema_guard.py) live
# alongside this test file...
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

# ...but they're always invoked with the repo root as cwd (e.g.
# `python3 submission/megha/cdc_extract.py` run from ~/data-assignments),
# so that's where they write lake/ and schema_snapshot.json. Tests must
# use that same root for both finding data files and setting subprocess cwd.
REPO_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, "..", ".."))


def script_path(name):
    return os.path.join(SCRIPTS_DIR, name)


@pytest.fixture
def conn():
    c = psycopg2.connect(**DB_CONFIG)
    c.autocommit = False
    yield c
    c.rollback()  # never persist test-only mutations
    c.close()


@pytest.fixture
def cur(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        yield c


# ------------------------------------------------------------------
# 1. Modeling / constraints
# ------------------------------------------------------------------

class TestSourceConstraints:
    def test_transfer_amount_must_be_positive(self, conn, cur):
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                "INSERT INTO transfer (source_wallet_id, dest_wallet_id, amount) "
                "VALUES (1, 2, -50.00)"
            )
        conn.rollback()

    def test_transfer_cannot_self_reference(self, conn, cur):
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                "INSERT INTO transfer (source_wallet_id, dest_wallet_id, amount) "
                "VALUES (1, 1, 50.00)"
            )
        conn.rollback()

    def test_settled_at_cannot_precede_created_at(self, conn, cur):
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                "INSERT INTO transfer "
                "(source_wallet_id, dest_wallet_id, amount, created_at, settled_at) "
                "VALUES (1, 2, 50.00, now(), now() - interval '1 day')"
            )
        conn.rollback()

    def test_customer_email_must_be_unique(self, conn, cur):
        cur.execute("SELECT email FROM customer LIMIT 1")
        existing_email = cur.fetchone()["email"]
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute(
                "INSERT INTO customer (name, email) VALUES ('Dup', %s)",
                (existing_email,),
            )
        conn.rollback()

    def test_payment_attempt_no_must_be_unique_per_transfer(self, conn, cur):
        cur.execute("SELECT id FROM transfer LIMIT 1")
        transfer_id = cur.fetchone()["id"]
        cur.execute(
            "SELECT attempt_no FROM payment_attempt WHERE transfer_id = %s LIMIT 1",
            (transfer_id,),
        )
        row = cur.fetchone()
        if row is None:
            pytest.skip("no existing payment_attempt to test uniqueness against")
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute(
                "INSERT INTO payment_attempt (transfer_id, attempt_no, status) "
                "VALUES (%s, %s, 'initiated')",
                (transfer_id, row["attempt_no"]),
            )
        conn.rollback()


# ------------------------------------------------------------------
# 2. CDC correctness
# ------------------------------------------------------------------

class TestCdcCorrectness:
    def test_extractor_is_idempotent_with_no_new_data(self):
        """Running the extractor twice in a row with no source changes
        should capture 0 records on the second run."""
        result = subprocess.run(
            [sys.executable, script_path("cdc_extract.py")],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "Done. 0 total change record(s)" in result.stdout, (
            "Expected no new changes on a repeat run with no source "
            f"mutations; got:\n{result.stdout}"
        )

    def test_lake_records_have_required_fields(self):
        path = os.path.join(REPO_ROOT, "lake", "wallet.jsonl")
        if not os.path.exists(path):
            pytest.skip("lake/wallet.jsonl not present — run cdc_extract.py first")
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) > 0
        for line in lines:
            record = json.loads(line)
            for field in ("table", "op", "pk", "updated_at", "captured_at", "data"):
                assert field in record, f"missing field {field} in {record}"
            assert record["op"] in ("upsert", "delete")

    def test_soft_deleted_row_captured_as_delete_op(self):
        path = os.path.join(REPO_ROOT, "lake", "wallet.jsonl")
        if not os.path.exists(path):
            pytest.skip("lake/wallet.jsonl not present — run cdc_extract.py first")
        with open(path) as f:
            records = [json.loads(line) for line in f]
        deleted = [r for r in records if r["data"].get("is_deleted") is True]
        for r in deleted:
            assert r["op"] == "delete", (
                f"row {r['pk']} has is_deleted=true but op={r['op']!r}"
            )


# ------------------------------------------------------------------
# 3. Warehouse correctness
# ------------------------------------------------------------------

class TestWarehouseCorrectness:
    def test_warehouse_wallet_count_matches_source(self, cur):
        cur.execute("SELECT count(*) AS n FROM wallet")
        source_count = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM warehouse.wallet")
        warehouse_count = cur.fetchone()["n"]
        assert source_count == warehouse_count

    def test_warehouse_snapshot_matches_source_for_each_wallet(self, cur):
        cur.execute("SELECT id, status, is_deleted FROM wallet")
        source_rows = {r["id"]: (r["status"], r["is_deleted"]) for r in cur.fetchall()}

        cur.execute("SELECT id, status, is_deleted FROM warehouse.wallet")
        warehouse_rows = {r["id"]: (r["status"], r["is_deleted"]) for r in cur.fetchall()}

        assert source_rows == warehouse_rows

    def test_scd2_history_has_exactly_one_current_row_per_entity(self, cur):
        cur.execute(
            """
            SELECT id, count(*) AS n
            FROM warehouse.wallet_history
            WHERE is_current = true
            GROUP BY id
            HAVING count(*) != 1
            """
        )
        violations = cur.fetchall()
        assert violations == [], f"entities with != 1 current row: {violations}"

    def test_scd2_closed_rows_have_valid_to_set(self, cur):
        cur.execute(
            "SELECT id, surrogate_id FROM warehouse.transfer_history "
            "WHERE is_current = false AND valid_to IS NULL"
        )
        violations = cur.fetchall()
        assert violations == [], f"closed rows missing valid_to: {violations}"

    def test_loader_is_idempotent(self):
        """Running the loader twice with no new lake records should load 0."""
        result = subprocess.run(
            [sys.executable, script_path("warehouse_load.py")],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "Done. 0 total record(s) loaded" in result.stdout, (
            f"Expected idempotent no-op load; got:\n{result.stdout}"
        )


# ------------------------------------------------------------------
# 4. Schema-change safety
# ------------------------------------------------------------------

class TestSchemaChangeSafety:
    def test_no_breaking_change_exits_zero(self):
        result = subprocess.run(
            [sys.executable, script_path("schema_guard.py")],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_dropped_column_is_detected_as_breaking(self, conn, cur):
        # Defensive cleanup: a previous failed run may have left this
        # column behind before reaching its own cleanup step.
        cur.execute("ALTER TABLE payment_attempt DROP COLUMN IF EXISTS temp_test_col")
        conn.commit()

        # Add a throwaway nullable column, initialize baseline to include it,
        # then drop it and confirm schema_guard flags it and exits non-zero.
        cur.execute("ALTER TABLE payment_attempt ADD COLUMN temp_test_col TEXT")
        conn.commit()

        init_result = subprocess.run(
            [sys.executable, script_path("schema_guard.py"), "--init"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert init_result.returncode == 0, init_result.stderr

        cur.execute("ALTER TABLE payment_attempt DROP COLUMN temp_test_col")
        conn.commit()

        check_result = subprocess.run(
            [sys.executable, script_path("schema_guard.py")],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert check_result.returncode == 1
        assert "temp_test_col" in check_result.stdout
        assert "BREAKING" in check_result.stdout

        # Restore baseline to real schema so later test runs aren't affected.
        subprocess.run(
            [sys.executable, script_path("schema_guard.py"), "--init"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
