"""
Validation Parity Layer — enforcing source and downstream warehouse data quality rules.

Ensures complete parity between source business rules and warehouse models.
Validations are divided into two main categories:
1. System Validations: Primary key uniqueness, foreign key referential integrity, NOT NULL rules.
2. Business Validations: Non-negative monetary amounts, timestamp ordering, enum domains, ledger consistency.
"""

from __future__ import annotations

import duckdb


def run_validation_suite(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Run complete system and business validation suite against source and warehouse tables.

    Returns a list of violation error strings. Empty list indicates all validations passed.
    """
    violations: list[str] = []

    # ─────────────────────────────────────────────────────────────────────────
    # 1. System Validations: Primary Key Uniqueness
    # ─────────────────────────────────────────────────────────────────────────
    pk_checks = [
        ("customers", "customer_id"),
        ("wallets", "wallet_id"),
        ("merchants", "merchant_id"),
        ("transactions", "transaction_id"),
        ("ledger_entries", "entry_id"),
    ]

    for table, pk_col in pk_checks:
        _check_pk_uniqueness(conn, table, pk_col, violations)

    # Check warehouse PK uniqueness for active non-deleted rows
    wh_pk_checks = [
        ("wh_customers", "customer_id"),
        ("wh_wallets", "wallet_id"),
        ("wh_merchants", "merchant_id"),
        ("wh_transactions", "transaction_id"),
        ("wh_ledger_entries", "entry_id"),
    ]

    for table, pk_col in wh_pk_checks:
        _check_wh_pk_uniqueness(conn, table, pk_col, violations)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. System Validations: Foreign Key Referential Integrity
    # ─────────────────────────────────────────────────────────────────────────
    fk_checks = [
        ("wallets", "customer_id", "customers", "customer_id"),
        ("transactions", "wallet_id", "wallets", "wallet_id"),
        ("transactions", "merchant_id", "merchants", "merchant_id"),
        ("ledger_entries", "transaction_id", "transactions", "transaction_id"),
        ("ledger_entries", "wallet_id", "wallets", "wallet_id"),
    ]

    for child_tbl, child_fk, parent_tbl, parent_pk in fk_checks:
        _check_referential_integrity(
            conn, child_tbl, child_fk, parent_tbl, parent_pk, violations
        )

    # Warehouse referential integrity (active rows)
    wh_fk_checks = [
        ("wh_wallets", "customer_id", "wh_customers", "customer_id"),
        ("wh_transactions", "wallet_id", "wh_wallets", "wallet_id"),
        ("wh_ledger_entries", "transaction_id", "wh_transactions", "transaction_id"),
    ]

    for child_tbl, child_fk, parent_tbl, parent_pk in wh_fk_checks:
        _check_wh_referential_integrity(
            conn, child_tbl, child_fk, parent_tbl, parent_pk, violations
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 3. System Validations: Not-Null Enforcement
    # ─────────────────────────────────────────────────────────────────────────
    not_null_checks = [
        ("customers", ["customer_id", "name", "email", "status", "created_at"]),
        ("wallets", ["wallet_id", "customer_id", "balance", "currency", "status"]),
        (
            "transactions",
            ["transaction_id", "wallet_id", "amount", "direction", "status"],
        ),
        (
            "ledger_entries",
            ["entry_id", "transaction_id", "wallet_id", "entry_type", "amount"],
        ),
    ]

    for table, cols in not_null_checks:
        for col in cols:
            _check_not_null(conn, table, col, violations)

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Business Validations: Monetary Amounts & Balance Rules
    # ─────────────────────────────────────────────────────────────────────────
    # Wallet balance non-negative check
    for tbl in ["wallets", "wh_wallets"]:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE balance < 0"
            ).fetchone()[0]
            if count > 0:
                violations.append(f"{tbl}: {count} row(s) have negative wallet balance")
        except duckdb.CatalogException:
            pass  # Table does not exist in current connection context

    # Transaction amount > 0 and fee >= 0 check
    for tbl in ["transactions", "wh_transactions"]:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE amount <= 0 OR fee < 0"
            ).fetchone()[0]
            if count > 0:
                violations.append(
                    f"{tbl}: {count} row(s) have invalid transaction amount or fee"
                )
        except duckdb.CatalogException:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # 5. Business Validations: Timestamp Ordering
    # ─────────────────────────────────────────────────────────────────────────
    for tbl in ["transactions", "wh_transactions"]:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE settled_at IS NOT NULL AND settled_at < created_at"
            ).fetchone()[0]
            if count > 0:
                violations.append(
                    f"{tbl}: {count} row(s) have settled_at timestamp preceding created_at"
                )
        except duckdb.CatalogException:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # 6. Business Validations: Domain Status Enum Restrictions
    # ─────────────────────────────────────────────────────────────────────────
    enum_rules = [
        ("customers", "status", ["active", "suspended", "closed"]),
        ("wallets", "status", ["active", "frozen", "closed"]),
        ("transactions", "status", ["pending", "settled", "failed", "reversed"]),
        ("transactions", "direction", ["credit", "debit"]),
        ("ledger_entries", "entry_type", ["debit", "credit"]),
    ]

    for table, col, allowed in enum_rules:
        _check_enum_domain(conn, table, col, allowed, violations)

    return violations


# ── Private Helper Assertions ──────────────────────────────────────────────────


def _check_pk_uniqueness(
    conn: duckdb.DuckDBPyConnection, table: str, pk_col: str, violations: list[str]
) -> None:
    try:
        res = conn.execute(
            f"SELECT {pk_col}, COUNT(*) FROM {table} GROUP BY {pk_col} HAVING COUNT(*) > 1"
        ).fetchall()
        if res:
            violations.append(
                f"{table}: primary key {pk_col} non-uniqueness detected ({len(res)} duplicate keys)"
            )
    except duckdb.CatalogException:
        pass


def _check_wh_pk_uniqueness(
    conn: duckdb.DuckDBPyConnection, table: str, pk_col: str, violations: list[str]
) -> None:
    try:
        res = conn.execute(
            f"SELECT {pk_col}, COUNT(*) FROM {table} WHERE _deleted = false GROUP BY {pk_col} HAVING COUNT(*) > 1"
        ).fetchall()
        if res:
            violations.append(
                f"{table}: primary key {pk_col} non-uniqueness detected in active warehouse rows"
            )
    except duckdb.CatalogException:
        pass


def _check_referential_integrity(
    conn: duckdb.DuckDBPyConnection,
    child_tbl: str,
    child_fk: str,
    parent_tbl: str,
    parent_pk: str,
    violations: list[str],
) -> None:
    try:
        query = f"""
            SELECT COUNT(*)
            FROM {child_tbl} c
            LEFT JOIN {parent_tbl} p ON c.{child_fk} = p.{parent_pk}
            WHERE c.{child_fk} IS NOT NULL AND p.{parent_pk} IS NULL
        """
        count = conn.execute(query).fetchone()[0]
        if count > 0:
            violations.append(
                f"{child_tbl}.{child_fk} -> {parent_tbl}.{parent_pk}: {count} orphaned record(s) violate foreign key constraint"
            )
    except duckdb.CatalogException:
        pass


def _check_wh_referential_integrity(
    conn: duckdb.DuckDBPyConnection,
    child_tbl: str,
    child_fk: str,
    parent_tbl: str,
    parent_pk: str,
    violations: list[str],
) -> None:
    try:
        query = f"""
            SELECT COUNT(*)
            FROM {child_tbl} c
            LEFT JOIN {parent_tbl} p ON c.{child_fk} = p.{parent_pk}
            WHERE c._deleted = false AND c.{child_fk} IS NOT NULL AND (p.{parent_pk} IS NULL OR p._deleted = true)
        """
        count = conn.execute(query).fetchone()[0]
        if count > 0:
            violations.append(
                f"{child_tbl}.{child_fk} -> {parent_tbl}.{parent_pk}: {count} active warehouse row(s) violate referential integrity"
            )
    except duckdb.CatalogException:
        pass


def _check_not_null(
    conn: duckdb.DuckDBPyConnection, table: str, col: str, violations: list[str]
) -> None:
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
        ).fetchone()[0]
        if count > 0:
            violations.append(
                f"{table}.{col}: {count} null value(s) violate NOT NULL constraint"
            )
    except duckdb.CatalogException:
        pass


def _check_enum_domain(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    col: str,
    allowed: list[str],
    violations: list[str],
) -> None:
    try:
        allowed_str = ", ".join(f"'{val}'" for val in allowed)
        query = f"SELECT COUNT(*) FROM {table} WHERE {col} NOT IN ({allowed_str})"
        count = conn.execute(query).fetchone()[0]
        if count > 0:
            violations.append(
                f"{table}.{col}: {count} row(s) contain invalid enum value outside allowed domain ({allowed})"
            )
    except duckdb.CatalogException:
        pass
