import pytest


@pytest.fixture
def sample_customer_row() -> dict:
    return {
        "customer_id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "phone": None,
        "status": "ACTIVE",
        "created_at": "2026-05-26T10:00:00",
        "updated_at": "2026-05-26T10:00:00",
        "is_deleted": 0,
    }
