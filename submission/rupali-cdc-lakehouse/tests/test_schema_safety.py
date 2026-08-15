from src.schema.safety import SchemaSafety


def test_matching_schema_is_compatible():
    expected = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "first_name": {"type": "VARCHAR", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": False},
        }
    }

    actual = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "first_name": {"type": "VARCHAR", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": False},
        }
    }

    checker = SchemaSafety(expected)

    assert checker.validate(actual) is True


def test_changed_data_type_is_incompatible():
    expected = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": False},
        }
    }

    actual = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "email": {"type": "BIGINT", "nullable": False},
        }
    }

    checker = SchemaSafety(expected)

    assert checker.validate(actual) is False


def test_changed_nullable_requirement_is_incompatible():
    expected = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": False},
        }
    }

    actual = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": True},
        }
    }

    checker = SchemaSafety(expected)

    assert checker.validate(actual) is False


def test_changed_primary_key_is_incompatible():
    expected = {
        "customers": {
            "customer_id": {
                "type": "BIGINT",
                "nullable": False,
                "primary_key": True,
            },
            "email": {
                "type": "VARCHAR",
                "nullable": False,
                "primary_key": False,
            },
        }
    }

    actual = {
        "customers": {
            "customer_id": {
                "type": "BIGINT",
                "nullable": False,
                "primary_key": False,
            },
            "email": {
                "type": "VARCHAR",
                "nullable": False,
                "primary_key": True,
            },
        }
    }

    checker = SchemaSafety(expected)

    assert checker.validate(actual) is False


def test_missing_column_is_incompatible():
    expected = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
            "email": {"type": "VARCHAR", "nullable": False},
        }
    }

    actual = {
        "customers": {
            "customer_id": {"type": "BIGINT", "nullable": False},
        }
    }

    checker = SchemaSafety(expected)

    assert checker.validate(actual) is False