"""
Source schema for the Global Commodity Trade & Cargo Logistics system.

Domain: Commodity importers, exporters, contracts, customs, and settlements.

Strong entities: trade_partners, trade_contracts
Weak entities  : shipment_cargo, customs_clearances, settlement_transactions
                 (all reference trade_contracts as parent)

Invariants:
- trade_contracts.total_value_usd = weight_metric_tons * price_per_ton_usd
- customs_clearances.duty_fee_usd >= 0
- settlement_transactions.payment_amount_usd > 0
- status fields restricted to known enum values
"""

import duckdb

# Expected columns per table — used by schema-contract checks.
SCHEMA_CONTRACT: dict[str, list[str]] = {
    "trade_partners": [
        "partner_id",
        "company_name",
        "country",
        "partner_type",
        "compliance_status",
        "created_at",
        "updated_at",
    ],
    "trade_contracts": [
        "contract_id",
        "seller_id",
        "buyer_id",
        "commodity_type",
        "weight_metric_tons",
        "price_per_ton_usd",
        "total_value_usd",
        "status",
        "created_at",
        "updated_at",
    ],
    "shipment_cargo": [
        "cargo_id",
        "contract_id",
        "carrier_name",
        "container_number",
        "port_of_loading",
        "port_of_discharge",
        "estimated_delivery",
        "actual_delivery",
    ],
    "customs_clearances": [
        "clearance_id",
        "contract_id",
        "clearing_country",
        "duty_fee_usd",
        "status",
        "cleared_at",
    ],
    "settlement_transactions": [
        "transaction_id",
        "contract_id",
        "payment_amount_usd",
        "settlement_type",
        "status",
        "settled_at",
    ],
}


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create source tables with constraints in the given DuckDB connection."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_partners (
            partner_id         VARCHAR PRIMARY KEY,
            company_name       VARCHAR NOT NULL,
            country            VARCHAR NOT NULL,
            partner_type       VARCHAR NOT NULL
                            CHECK (partner_type IN ('buyer', 'seller')),
            compliance_status  VARCHAR NOT NULL
                            CHECK (compliance_status IN ('active', 'suspended', 'under_review')),
            created_at         TIMESTAMP NOT NULL,
            updated_at         TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_contracts (
            contract_id         VARCHAR PRIMARY KEY,
            seller_id           VARCHAR NOT NULL REFERENCES trade_partners(partner_id),
            buyer_id            VARCHAR NOT NULL REFERENCES trade_partners(partner_id),
            commodity_type      VARCHAR NOT NULL,
            weight_metric_tons  DECIMAL(18, 4) NOT NULL CHECK (weight_metric_tons > 0),
            price_per_ton_usd   DECIMAL(18, 2) NOT NULL CHECK (price_per_ton_usd > 0),
            total_value_usd     DECIMAL(18, 2) NOT NULL CHECK (total_value_usd > 0),
            status              VARCHAR NOT NULL
                            CHECK (status IN ('pending', 'active', 'completed', 'cancelled')),
            created_at          TIMESTAMP NOT NULL,
            updated_at          TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS shipment_cargo (
            cargo_id            VARCHAR PRIMARY KEY,
            contract_id         VARCHAR NOT NULL REFERENCES trade_contracts(contract_id),
            carrier_name        VARCHAR NOT NULL,
            container_number    VARCHAR NOT NULL,
            port_of_loading     VARCHAR NOT NULL,
            port_of_discharge    VARCHAR NOT NULL,
            estimated_delivery  TIMESTAMP NOT NULL,
            actual_delivery     TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customs_clearances (
            clearance_id      VARCHAR PRIMARY KEY,
            contract_id       VARCHAR NOT NULL REFERENCES trade_contracts(contract_id),
            clearing_country  VARCHAR NOT NULL,
            duty_fee_usd      DECIMAL(18, 2) NOT NULL CHECK (duty_fee_usd >= 0),
            status            VARCHAR NOT NULL
                           CHECK (status IN ('pending', 'approved', 'held_in_customs', 'rejected')),
            cleared_at        TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS settlement_transactions (
            transaction_id      VARCHAR PRIMARY KEY,
            contract_id         VARCHAR NOT NULL REFERENCES trade_contracts(contract_id),
            payment_amount_usd  DECIMAL(18, 2) NOT NULL CHECK (payment_amount_usd > 0),
            settlement_type     VARCHAR NOT NULL
                            CHECK (settlement_type IN ('wire_transfer', 'letter_of_credit')),
            status              VARCHAR NOT NULL
                            CHECK (status IN ('pending', 'settled', 'failed')),
            settled_at          TIMESTAMP
        )
    """)
