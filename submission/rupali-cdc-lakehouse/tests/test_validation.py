from src.validation.validator import WarehouseValidator
from src.warehouse.writer import WarehouseWriter


def test_primary_key_uniqueness_passes_for_unique_records(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    writer.connection.execute(
        """
        CREATE TABLE customers (
            customer_id BIGINT,
            first_name VARCHAR
        )
        """
    )

    writer.connection.execute(
        """
        INSERT INTO customers VALUES
        (1, 'Rupali'),
        (2, 'Test')
        """
    )

    validator = WarehouseValidator(writer)

    result = validator.validate_primary_key("customers", "customer_id")

    assert result["valid"] is True
    assert result["duplicate_count"] == 0


def test_primary_key_uniqueness_fails_for_duplicate_records(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    writer.connection.execute(
        """
        CREATE TABLE customers (
            customer_id BIGINT,
            first_name VARCHAR
        )
        """
    )

    writer.connection.execute(
        """
        INSERT INTO customers VALUES
        (1, 'Rupali'),
        (1, 'Duplicate')
        """
    )

    validator = WarehouseValidator(writer)

    result = validator.validate_primary_key("customers", "customer_id")

    assert result["valid"] is False
    assert result["duplicate_count"] == 1


def test_foreign_key_integrity_passes(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    writer.connection.execute(
        """
        CREATE TABLE customers (
            customer_id BIGINT
        )
        """
    )

    writer.connection.execute(
        """
        CREATE TABLE orders (
            order_id BIGINT,
            customer_id BIGINT
        )
        """
    )

    writer.connection.execute(
        """
        INSERT INTO customers VALUES (1)
        """
    )

    writer.connection.execute(
        """
        INSERT INTO orders VALUES (100, 1)
        """
    )

    validator = WarehouseValidator(writer)

    result = validator.validate_foreign_key(
        "orders",
        "customer_id",
        "customers",
        "customer_id",
    )

    assert result["valid"] is True
    assert result["invalid_count"] == 0


def test_foreign_key_integrity_fails_for_orphan_record(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    writer.connection.execute(
        """
        CREATE TABLE customers (
            customer_id BIGINT
        )
        """
    )

    writer.connection.execute(
        """
        CREATE TABLE orders (
            order_id BIGINT,
            customer_id BIGINT
        )
        """
    )

    writer.connection.execute(
        """
        INSERT INTO customers VALUES (1)
        """
    )

    writer.connection.execute(
        """
        INSERT INTO orders VALUES
        (100, 1),
        (101, 999)
        """
    )

    validator = WarehouseValidator(writer)

    result = validator.validate_foreign_key(
        "orders",
        "customer_id",
        "customers",
        "customer_id",
    )

    assert result["valid"] is False
    assert result["invalid_count"] == 1


def test_validate_all_fails_when_any_validation_fails(tmp_path):
    writer = WarehouseWriter(tmp_path / "warehouse.duckdb")

    writer.connection.execute(
        """
        CREATE TABLE customers (
            customer_id BIGINT
        )
        """
    )

    writer.connection.execute(
        """
        INSERT INTO customers VALUES
        (1),
        (1)
        """
    )

    validator = WarehouseValidator(writer)

    result = validator.validate_all(
        primary_keys=[
            ("customers", "customer_id"),
        ],
        foreign_keys=[],
    )

    assert result["valid"] is False