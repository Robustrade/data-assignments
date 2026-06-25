#!/usr/bin/env python3
"""
run_data_quality_checks.py

CI entry point for the data-quality validation parity check
(`data-quality-check`).

The script:
  1. bootstraps an in-memory DuckDB,
  2. seeds it with the canonical wallet domain seed,
  3. runs every SYSTEM and BUSINESS check defined in this module,
  4. prints the rule name + table when a check fails,
  5. exits non-zero on any failure.

Each rule below is traceable to a source-side invariant (see the
docstrings in source/models.py and source/contracts.py) — that is the
"validation parity" the assignment asks for.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataclasses import dataclass
from decimal import Decimal

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.orchestrator import bootstrap, run_tick
from source.contracts import CONTRACTS
from source.models import ALLOWED_TRANSFER_TRANSITIONS, create_source_tables
from source.seed import build_seed_rows


# ── rule registry ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Rule:
    rule_id: str
    category: str         # 'system' | 'business' | 'freshness'
    description: str
    sql: str              # query that returns 0 rows when the rule passes


def _system_rules() -> list[Rule]:
    rules: list[Rule] = []

    # PK uniqueness
    for table, contract in CONTRACTS.items():
        wh = f"wh_{table}"
        pk = contract.primary_key
        rules.append(Rule(
            rule_id=f"sys.pk_unique.{table}",
            category="system",
            description=f"{wh}.{pk} must be unique",
            sql=f"""
                SELECT {pk}, COUNT(*) AS n
                FROM {wh}
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            """,
        ))

    # NOT NULL parity
    for table, contract in CONTRACTS.items():
        wh = f"wh_{table}"
        for col in contract.columns:
            if col.nullable:
                continue
            rules.append(Rule(
                rule_id=f"sys.not_null.{table}.{col.name}",
                category="system",
                description=f"{wh}.{col.name} must not be null",
                sql=f"SELECT 1 FROM {wh} "
                    f"WHERE {col.name} IS NULL AND _deleted = FALSE",
            ))

    # Enum domain parity (only on currently-present rows)
    for table, contract in CONTRACTS.items():
        wh = f"wh_{table}"
        for col, allowed in contract.enum_domains:
            allowed_sql = ", ".join(f"'{v}'" for v in sorted(allowed))
            rules.append(Rule(
                rule_id=f"sys.enum.{table}.{col}",
                category="system",
                description=f"{wh}.{col} must be one of {sorted(allowed)}",
                sql=f"SELECT 1 FROM {wh} "
                    f"WHERE _deleted = FALSE "
                    f"  AND {col} IS NOT NULL "
                    f"  AND {col} NOT IN ({allowed_sql})",
            ))

    # Referential integrity for present (non-deleted) rows
    for table, contract in CONTRACTS.items():
        wh = f"wh_{table}"
        for fk_col, parent, parent_col in contract.foreign_keys:
            parent_wh = f"wh_{parent}"
            rules.append(Rule(
                rule_id=f"sys.fk.{table}.{fk_col}",
                category="system",
                description=(
                    f"{wh}.{fk_col} must reference an existing "
                    f"{parent_wh}.{parent_col}"
                ),
                sql=f"""
                    SELECT 1
                    FROM   {wh} c
                    LEFT JOIN {parent_wh} p
                           ON c.{fk_col} = p.{parent_col}
                          AND p._deleted = FALSE
                    WHERE  c._deleted = FALSE
                      AND  c.{fk_col} IS NOT NULL
                      AND  p.{parent_col} IS NULL
                """,
            ))

    return rules


def _business_rules() -> list[Rule]:
    return [
        Rule(
            "biz.wallet.balance_non_negative",
            "business",
            "wh_wallets.balance must be >= 0",
            "SELECT 1 FROM wh_wallets "
            "WHERE _deleted = FALSE AND balance < 0",
        ),
        Rule(
            "biz.transfer.amount_positive",
            "business",
            "wh_transfers.amount must be > 0",
            "SELECT 1 FROM wh_transfers "
            "WHERE _deleted = FALSE AND amount <= 0",
        ),
        Rule(
            "biz.transfer.no_self_transfer",
            "business",
            "wh_transfers.from_wallet_id must differ from to_wallet_id",
            "SELECT 1 FROM wh_transfers "
            "WHERE _deleted = FALSE AND from_wallet_id = to_wallet_id",
        ),
        Rule(
            "biz.transfer.settled_at_after_created",
            "business",
            "wh_transfers.settled_at must be >= created_at when present",
            "SELECT 1 FROM wh_transfers "
            "WHERE _deleted = FALSE "
            "  AND settled_at IS NOT NULL "
            "  AND settled_at < created_at",
        ),
        Rule(
            "biz.transfer.settled_implies_settled_at",
            "business",
            "wh_transfers.settled_at must be set whenever status = 'settled'",
            "SELECT 1 FROM wh_transfers "
            "WHERE _deleted = FALSE "
            "  AND status = 'settled' AND settled_at IS NULL",
        ),
        Rule(
            "biz.transfer.failed_implies_reason",
            "business",
            "wh_transfers.failure_reason must be set whenever status = 'failed'",
            "SELECT 1 FROM wh_transfers "
            "WHERE _deleted = FALSE "
            "  AND status = 'failed' AND failure_reason IS NULL",
        ),
        Rule(
            "biz.ledger.signed_amount_consistent",
            "business",
            "ledger_entries.signed_amount must equal +/- amount per direction",
            "SELECT 1 FROM wh_ledger_entries "
            "WHERE _deleted = FALSE "
            "  AND ((direction = 'credit' AND signed_amount <> amount) "
            "    OR (direction = 'debit'  AND signed_amount <> -amount))",
        ),
        Rule(
            "biz.ledger.balance_after_non_negative",
            "business",
            "ledger_entries.balance_after must be >= 0",
            "SELECT 1 FROM wh_ledger_entries "
            "WHERE _deleted = FALSE AND balance_after < 0",
        ),
        # Monetary integrity — ledger sums per wallet must equal wallet balance.
        Rule(
            "biz.ledger.reconciles_to_wallet_balance",
            "business",
            "SUM(ledger.signed_amount) per wallet must equal wallet.balance",
            """
            WITH ledger_sum AS (
                SELECT wallet_id, SUM(signed_amount) AS s
                FROM   wh_ledger_entries
                WHERE  _deleted = FALSE
                GROUP BY wallet_id
            )
            SELECT 1
            FROM   wh_wallets w
            LEFT JOIN ledger_sum l ON l.wallet_id = w.wallet_id
            WHERE  w._deleted = FALSE
              AND  COALESCE(l.s, 0) <> w.balance
            """,
        ),
        # Status transitions must follow the documented state machine.
        Rule(
            "biz.transfer_event.allowed_transition",
            "business",
            "transfer_events.from_status -> to_status must be allowed "
            "(or from_status NULL for the first event)",
            f"""
            SELECT 1 FROM wh_transfer_events
            WHERE _deleted = FALSE
              AND from_status IS NOT NULL
              AND (from_status, to_status) NOT IN (
                {", ".join(f"('{f}', '{t}')" for f, t in
                           sorted(ALLOWED_TRANSFER_TRANSITIONS))}
              )
            """,
        ),
    ]


def all_rules() -> list[Rule]:
    return _system_rules() + _business_rules()


# ── seed + run ───────────────────────────────────────────────────────────────


def _seed_pipeline(conn: duckdb.DuckDBPyConnection) -> CDCCapture:
    create_source_tables(conn)
    bootstrap(conn)
    capture = CDCCapture()
    for row in build_seed_rows():
        capture.insert(row.table, row.pk, row.data)
    run_tick(conn, capture)
    return capture


def run_checks(conn: duckdb.DuckDBPyConnection) -> list[tuple[Rule, int]]:
    """Return list of (rule, n_failing_rows) for any rule that fails."""
    failures: list[tuple[Rule, int]] = []
    for rule in all_rules():
        try:
            n = len(conn.execute(rule.sql).fetchall())
        except Exception as exc:
            failures.append((rule, -1))
            print(f"  ! rule errored: {rule.rule_id} — {exc}")
            continue
        if n > 0:
            failures.append((rule, n))
    return failures


def main() -> int:
    conn = duckdb.connect(":memory:")
    _seed_pipeline(conn)

    failures = run_checks(conn)

    if failures:
        print("Data quality failures:")
        for rule, n in failures:
            tag = "?" if n < 0 else f"{n} rows"
            print(f"  x [{rule.category}] {rule.rule_id}: {rule.description} "
                  f"({tag})")
        return 1

    n_rules = len(all_rules())
    print(f"All data quality checks passed ({n_rules} rules evaluated).")
    return 0


if __name__ == "__main__":
    # Reference Decimal so the import is "used" for static analysers when
    # rules grow into Python-side numeric tolerance checks.
    _ = Decimal("0")
    sys.exit(main())
