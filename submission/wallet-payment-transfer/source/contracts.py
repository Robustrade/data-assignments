"""
Versioned schema contracts for the source system.

Each contract describes what the CDC pipeline depends on for a given source
table:
  - column name -> {type, nullable}
  - primary key
  - foreign keys
  - enum domains (subset of allowed values)
  - contract version

The contracts are explicit so the pipeline can fail closed when the source
deviates from them. Adding new optional columns is *additive* and does not
break the contract; the rules in pipeline/schema_detector.py treat:

  * dropped columns                       -> BREAKING
  * type widening / narrowing             -> BREAKING
  * enum domain *shrunk*                  -> BREAKING (existing rows may rely
                                             on a value the pipeline just lost)
  * nullability tightened (allow -> deny) -> BREAKING (rows the pipeline
                                             previously emitted may now fail)
  * primary key changed                   -> BREAKING (replay/upsert depends
                                             on the original PK)
  * new nullable columns                  -> WARN  (additive, allow-listed)

The contract is the input the warehouse modeling layer is allowed to assume.
Anything outside the contract is best-effort.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type: str
    nullable: bool


@dataclass(frozen=True)
class TableContract:
    name: str
    columns: tuple[ColumnContract, ...]
    primary_key: str
    foreign_keys: tuple[tuple[str, str, str], ...] = field(default_factory=tuple)
    # (column, referenced_table, referenced_column)
    enum_domains: tuple[tuple[str, frozenset[str]], ...] = field(
        default_factory=tuple
    )
    contract_version: int = 1

    def column(self, name: str) -> Optional[ColumnContract]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


# NOTE: the type strings below are normalized — DuckDB DESCRIBE returns the
# normalised type names we compare against in pipeline/schema_detector.py.
CONTRACTS: dict[str, TableContract] = {
    "customers": TableContract(
        name="customers",
        primary_key="customer_id",
        columns=(
            ColumnContract("customer_id",       "VARCHAR",   False),
            ColumnContract("full_name",         "VARCHAR",   False),
            ColumnContract("email",             "VARCHAR",   False),
            ColumnContract("country_code",      "VARCHAR",   False),
            ColumnContract("status",            "VARCHAR",   False),
            ColumnContract("registration_date", "DATE",      False),
            ColumnContract("created_at",        "TIMESTAMP", False),
            ColumnContract("updated_at",        "TIMESTAMP", False),
        ),
        enum_domains=(
            ("status", frozenset({"active", "suspended", "closed"})),
        ),
    ),
    "payment_methods": TableContract(
        name="payment_methods",
        primary_key="payment_method_id",
        columns=(
            ColumnContract("payment_method_id", "VARCHAR",   False),
            ColumnContract("customer_id",       "VARCHAR",   False),
            ColumnContract("method_type",       "VARCHAR",   False),
            ColumnContract("masked_identifier", "VARCHAR",   False),
            ColumnContract("is_default",        "BOOLEAN",   False),
            ColumnContract("status",            "VARCHAR",   False),
            ColumnContract("created_at",        "TIMESTAMP", False),
            ColumnContract("updated_at",        "TIMESTAMP", False),
        ),
        foreign_keys=(("customer_id", "customers", "customer_id"),),
        enum_domains=(
            ("method_type", frozenset({"card", "bank", "upi"})),
            ("status",      frozenset({"active", "expired", "revoked"})),
        ),
    ),
    "wallets": TableContract(
        name="wallets",
        primary_key="wallet_id",
        columns=(
            ColumnContract("wallet_id",   "VARCHAR",       False),
            ColumnContract("customer_id", "VARCHAR",       False),
            ColumnContract("currency",    "VARCHAR",       False),
            ColumnContract("balance",     "DECIMAL(18,4)", False),
            ColumnContract("status",      "VARCHAR",       False),
            ColumnContract("created_at",  "TIMESTAMP",     False),
            ColumnContract("updated_at",  "TIMESTAMP",     False),
        ),
        foreign_keys=(("customer_id", "customers", "customer_id"),),
        enum_domains=(
            ("currency", frozenset({"USD", "EUR", "GBP", "INR", "JPY"})),
            ("status",   frozenset({"active", "frozen", "closed"})),
        ),
    ),
    "transfers": TableContract(
        name="transfers",
        primary_key="transfer_id",
        columns=(
            ColumnContract("transfer_id",       "VARCHAR",       False),
            ColumnContract("from_wallet_id",    "VARCHAR",       False),
            ColumnContract("to_wallet_id",      "VARCHAR",       False),
            ColumnContract("payment_method_id", "VARCHAR",       True),
            ColumnContract("amount",            "DECIMAL(18,4)", False),
            ColumnContract("currency",          "VARCHAR",       False),
            ColumnContract("status",            "VARCHAR",       False),
            ColumnContract("reference",         "VARCHAR",       True),
            ColumnContract("failure_reason",    "VARCHAR",       True),
            ColumnContract("created_at",        "TIMESTAMP",     False),
            ColumnContract("settled_at",        "TIMESTAMP",     True),
        ),
        foreign_keys=(
            ("from_wallet_id",    "wallets",         "wallet_id"),
            ("to_wallet_id",      "wallets",         "wallet_id"),
            ("payment_method_id", "payment_methods", "payment_method_id"),
        ),
        enum_domains=(
            ("status",   frozenset({"pending", "settled", "failed", "reversed"})),
            ("currency", frozenset({"USD", "EUR", "GBP", "INR", "JPY"})),
        ),
    ),
    "transfer_events": TableContract(
        name="transfer_events",
        primary_key="transfer_event_id",
        columns=(
            ColumnContract("transfer_event_id", "VARCHAR",   False),
            ColumnContract("transfer_id",       "VARCHAR",   False),
            ColumnContract("from_status",       "VARCHAR",   True),
            ColumnContract("to_status",         "VARCHAR",   False),
            ColumnContract("note",              "VARCHAR",   True),
            ColumnContract("occurred_at",       "TIMESTAMP", False),
        ),
        foreign_keys=(("transfer_id", "transfers", "transfer_id"),),
        enum_domains=(
            ("to_status", frozenset({"pending", "settled", "failed", "reversed"})),
        ),
    ),
    "ledger_entries": TableContract(
        name="ledger_entries",
        primary_key="ledger_entry_id",
        columns=(
            ColumnContract("ledger_entry_id", "VARCHAR",       False),
            ColumnContract("wallet_id",       "VARCHAR",       False),
            ColumnContract("transfer_id",     "VARCHAR",       True),
            ColumnContract("direction",       "VARCHAR",       False),
            ColumnContract("amount",          "DECIMAL(18,4)", False),
            ColumnContract("signed_amount",   "DECIMAL(18,4)", False),
            ColumnContract("balance_after",   "DECIMAL(18,4)", False),
            ColumnContract("occurred_at",     "TIMESTAMP",     False),
        ),
        foreign_keys=(
            ("wallet_id",   "wallets",   "wallet_id"),
            ("transfer_id", "transfers", "transfer_id"),
        ),
        enum_domains=(
            ("direction", frozenset({"credit", "debit"})),
        ),
    ),
}


def expected_columns(table: str) -> list[str]:
    """Convenience for the schema contract CI script."""
    return [c.name for c in CONTRACTS[table].columns]
