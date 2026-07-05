"""Shared pytest fixtures for the logistics CDC pipeline tests."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import duckdb
import pytest

from pipeline.cdc import CDCCapture
from pipeline.lake import append_to_lake, create_lake_table
from pipeline.warehouse import apply_cdc_records, create_warehouse_tables
from source.models import create_source_tables


def _ts() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def conn() -> duckdb.DuckDBPyConnection:
    """Fresh in-memory DuckDB with source + lake + warehouse tables."""
    c = duckdb.connect(":memory:")
    create_source_tables(c)
    create_lake_table(c)
    create_warehouse_tables(c)
    return c


@pytest.fixture()
def capture() -> CDCCapture:
    """Empty CDC capture log."""
    return CDCCapture()


@pytest.fixture()
def seeded(conn: duckdb.DuckDBPyConnection, capture: CDCCapture):
    """
    DuckDB connection pre-loaded with trade partners, active contract,
    cargo, customs clearance, and a settlement transaction.
    """
    ts = _ts()

    # 1. Seed Partners
    capture.insert(
        "trade_partners",
        "p1",
        {
            "partner_id": "p1",
            "company_name": "Indo Exporters Ltd",
            "country": "Indonesia",
            "partner_type": "seller",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )
    capture.insert(
        "trade_partners",
        "p2",
        {
            "partner_id": "p2",
            "company_name": "Robustrade India",
            "country": "India",
            "partner_type": "buyer",
            "compliance_status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    # 2. Seed active contract
    capture.insert(
        "trade_contracts",
        "con1",
        {
            "contract_id": "con1",
            "seller_id": "p1",
            "buyer_id": "p2",
            "commodity_type": "Cocoa Beans",
            "weight_metric_tons": Decimal("50.0000"),
            "price_per_ton_usd": Decimal("2500.00"),
            "total_value_usd": Decimal("125000.00"),
            "status": "active",
            "created_at": ts,
            "updated_at": ts,
        },
    )

    # 3. Seed cargo
    capture.insert(
        "shipment_cargo",
        "car1",
        {
            "cargo_id": "car1",
            "contract_id": "con1",
            "carrier_name": "MSC Logistics",
            "container_number": "MSCU1234567",
            "port_of_loading": "Jakarta",
            "port_of_discharge": "Nhava Sheva",
            "estimated_delivery": ts + timedelta(days=10),
            "actual_delivery": None,
        },
    )

    # 4. Seed customs
    capture.insert(
        "customs_clearances",
        "cl1",
        {
            "clearance_id": "cl1",
            "contract_id": "con1",
            "clearing_country": "India",
            "duty_fee_usd": Decimal("2500.00"),
            "status": "pending",
            "cleared_at": None,
        },
    )

    # 5. Seed settlement transaction
    capture.insert(
        "settlement_transactions",
        "tx1",
        {
            "transaction_id": "tx1",
            "contract_id": "con1",
            "payment_amount_usd": Decimal("50000.00"),
            "settlement_type": "letter_of_credit",
            "status": "settled",
            "settled_at": ts,
        },
    )

    records = capture.records_since(0)
    append_to_lake(conn, records)
    apply_cdc_records(conn, records)
    return conn, capture
