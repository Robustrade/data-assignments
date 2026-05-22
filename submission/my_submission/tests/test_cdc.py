"""
test_cdc.py

Tests for CDC capture correctness (Requirement 2: CDC Pipeline).

Validates:
- INSERT operations are captured
- UPDATE operations are captured
- DELETE operations are captured
- Sequence numbers are monotonic
- Replay is deterministic
"""

import pytest
from datetime import datetime, timezone

from pipeline.cdc import CDCCapture, CDCRecord, VALID_OPERATIONS


class TestCDCCapture:
    """Test CDC capture mechanics."""

    def test_cdc_insert_record(self, cdc_capture, sample_customer):
        """GIVEN customer data, WHEN capturing INSERT, THEN CDCRecord created with sequence 1."""
        record = cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        
        assert record.operation == "insert"
        assert record.table == "customers"
        assert record.primary_key == "CUST001"
        assert record.data == sample_customer
        assert record.sequence == 1

    def test_cdc_update_record(self, cdc_capture, sample_customer):
        """GIVEN existing customer, WHEN capturing UPDATE, THEN sequence increments."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        
        updated_customer = sample_customer.copy()
        updated_customer["status"] = "suspended"
        
        record = cdc_capture.update("customers", sample_customer["customer_id"], updated_customer)
        
        assert record.operation == "update"
        assert record.sequence == 2
        assert record.data["status"] == "suspended"

    def test_cdc_delete_record(self, cdc_capture, sample_customer):
        """GIVEN existing customer, WHEN capturing DELETE, THEN sequence increments."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        record = cdc_capture.delete("customers", sample_customer["customer_id"], sample_customer)
        
        assert record.operation == "delete"
        assert record.sequence == 2

    def test_cdc_sequence_monotonic(self, cdc_capture, sample_customer, sample_product):
        """GIVEN multiple operations, WHEN capturing changes, THEN sequences are monotonic."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        cdc_capture.insert("products", sample_product["product_id"], sample_product)
        cdc_capture.update("customers", sample_customer["customer_id"], sample_customer)
        
        assert cdc_capture.latest_sequence == 3

    def test_cdc_records_since_offset(self, cdc_capture, sample_customer, sample_product):
        """GIVEN checkpoint at sequence 1, WHEN replaying from offset, THEN only new records returned."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        cdc_capture.insert("products", sample_product["product_id"], sample_product)
        
        # Checkpoint after first record
        new_records = cdc_capture.records_since(offset=1)
        
        assert len(new_records) == 1
        assert new_records[0].table == "products"
        assert new_records[0].sequence == 2

    def test_cdc_replay_from_zero(self, cdc_capture, sample_customer, sample_product):
        """GIVEN CDC log, WHEN replaying from offset 0, THEN all records returned."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        cdc_capture.insert("products", sample_product["product_id"], sample_product)
        
        all_records = cdc_capture.records_since(offset=0)
        
        assert len(all_records) == 2

    def test_cdc_invalid_operation(self, cdc_capture, sample_customer):
        """GIVEN invalid operation, WHEN creating CDCRecord, THEN ValueError raised."""
        with pytest.raises(ValueError, match="Invalid CDC operation"):
            CDCRecord(
                operation="upsert",  # Invalid
                table="customers",
                primary_key="CUST001",
                data=sample_customer,
            )

    def test_cdc_log_immutable(self, cdc_capture, sample_customer):
        """GIVEN CDC log, WHEN retrieving log copy, THEN modifications don't affect original."""
        cdc_capture.insert("customers", sample_customer["customer_id"], sample_customer)
        
        log_copy = cdc_capture.log
        log_copy.append(None)  # Modify copy
        
        assert len(cdc_capture.log) == 1  # Original unchanged
