"""
Tests for historical recovery and time travel.

Validates:
- Lake preserves all CDC changes
- Replay from checkpoint works
- Point-in-time state can be reconstructed from lake events
- Warehouse current-state tables can be restored from lake sequence replay
"""

from datetime import datetime, timezone

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.warehouse import (
    apply_cdc_records,
    create_warehouse_tables,
    get_state_at_point_in_time,
    restore_warehouse_to_sequence,
)


class TestPointInTimeRecovery:
    """Test historical recovery and lake replay."""

    def test_lake_preserves_all_changes(self, setup_db, sample_customer):
        """GIVEN multiple CDC events, WHEN appended to lake, THEN all are retained."""
        create_lake_table(setup_db)
        cdc = CDCCapture()

        cdc.insert("customers", sample_customer["customer_id"], sample_customer)

        updated = sample_customer.copy()
        updated["status"] = "suspended"
        cdc.update("customers", sample_customer["customer_id"], updated)

        updated = updated.copy()
        updated["status"] = "closed"
        cdc.update("customers", sample_customer["customer_id"], updated)

        cdc.delete("customers", sample_customer["customer_id"], updated)

        assert append_to_lake(setup_db, cdc.log) == 4

        lake_count = setup_db.execute(
            "SELECT COUNT(*) FROM lake_cdc_events WHERE table_name = ?",
            ["customers"],
        ).fetchone()[0]
        assert lake_count == 4

    def test_replay_from_checkpoint(self, sample_customer):
        """GIVEN a checkpoint, WHEN replaying, THEN only later records return."""
        cdc = CDCCapture()

        cdc.insert("customers", sample_customer["customer_id"], sample_customer)
        updated = sample_customer.copy()
        updated["status"] = "suspended"
        cdc.update("customers", sample_customer["customer_id"], updated)

        updated = updated.copy()
        updated["status"] = "closed"
        cdc.update("customers", sample_customer["customer_id"], updated)

        unprocessed = cdc.records_since(offset=1)

        assert [record.sequence for record in unprocessed] == [2, 3]

    def test_point_in_time_query_replays_lake(self, setup_db, sample_customer):
        """GIVEN lake history, WHEN querying a timestamp, THEN correct state returns."""
        create_warehouse_tables(setup_db)

        cdc = CDCCapture()
        times = [
            datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc),
        ]
        statuses = ["active", "suspended", "active"]

        for i, (captured_at, status) in enumerate(zip(times, statuses)):
            customer = sample_customer.copy()
            customer["status"] = status
            if i == 0:
                record = cdc.insert("customers", sample_customer["customer_id"], customer)
            else:
                record = cdc.update("customers", sample_customer["customer_id"], customer)
            record.captured_at = captured_at

        append_to_lake(setup_db, cdc.log)
        apply_cdc_records(setup_db, cdc.log)

        state_at_11 = get_state_at_point_in_time(
            setup_db,
            "wh_customers",
            datetime(2026, 5, 1, 11, 0, tzinfo=timezone.utc),
        )
        state_at_13 = get_state_at_point_in_time(
            setup_db,
            "wh_customers",
            datetime(2026, 5, 1, 13, 0, tzinfo=timezone.utc),
        )
        state_at_15 = get_state_at_point_in_time(
            setup_db,
            "wh_customers",
            datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc),
        )

        assert state_at_11[0]["status"] == "active"
        assert state_at_13[0]["status"] == "suspended"
        assert state_at_15[0]["status"] == "active"

    def test_restore_to_sequence_replays_lake(self, setup_db, sample_customer):
        """GIVEN lake history, WHEN restoring sequence 2, THEN warehouse matches it."""
        create_warehouse_tables(setup_db)

        cdc = CDCCapture()
        statuses = ["active", "suspended", "closed"]

        for i, status in enumerate(statuses):
            customer = sample_customer.copy()
            customer["status"] = status
            if i == 0:
                cdc.insert("customers", sample_customer["customer_id"], customer)
            else:
                cdc.update("customers", sample_customer["customer_id"], customer)

        append_to_lake(setup_db, cdc.log)
        apply_cdc_records(setup_db, cdc.log)

        current = setup_db.execute(
            "SELECT status FROM wh_customers WHERE customer_id = ?",
            [sample_customer["customer_id"]],
        ).fetchone()
        assert current[0] == "closed"

        restore_warehouse_to_sequence(setup_db, 2)

        restored = setup_db.execute(
            "SELECT status FROM wh_customers WHERE customer_id = ?",
            [sample_customer["customer_id"]],
        ).fetchone()
        assert restored[0] == "suspended"

    def test_multiple_table_timeline(self, setup_db, sample_customer, sample_order):
        """GIVEN changes across tables, WHEN captured, THEN sequences preserve order."""
        create_lake_table(setup_db)
        create_warehouse_tables(setup_db)

        cdc = CDCCapture()

        cdc.insert("customers", sample_customer["customer_id"], sample_customer)
        cdc.insert("orders", sample_order["order_id"], sample_order)
        updated_customer = sample_customer.copy()
        updated_customer["status"] = "suspended"
        cdc.update("customers", sample_customer["customer_id"], updated_customer)

        assert [record.sequence for record in cdc.log] == [1, 2, 3]
        assert len([record for record in cdc.log if record.table == "customers"]) == 2
        assert len([record for record in cdc.log if record.table == "orders"]) == 1

    def test_lake_deduplication_safety(self, setup_db, sample_customer):
        """GIVEN a retry, WHEN same sequence is appended, THEN lake stores it once."""
        create_lake_table(setup_db)
        cdc = CDCCapture()

        record = cdc.insert("customers", sample_customer["customer_id"], sample_customer)

        assert append_to_lake(setup_db, [record]) == 1
        assert append_to_lake(setup_db, [record]) == 0

        count = setup_db.execute(
            "SELECT COUNT(*) FROM lake_cdc_events WHERE sequence = ?",
            [record.sequence],
        ).fetchone()[0]
        assert count == 1
