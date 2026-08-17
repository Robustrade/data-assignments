"""
Validation / Quality Layer.

Mirrors source system + business validations against the warehouse's
*_current tables. Each function returns a list of violation strings (empty
list == pass). Used both by pytest tests and can be run standalone as a
data-quality report.
"""
import json
import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
WAREHOUSE_PATH = BASE / "data" / "warehouse.duckdb"


def _rows(con, table):
    recs = con.execute(f"SELECT pk_key, row_data FROM {table}_current").fetchall()
    return [(pk, json.loads(row)) for pk, row in recs]


def check_pk_uniqueness(con) -> list[str]:
    violations = []
    for t in ["customers", "wallets", "transactions", "transaction_line_items", "balance_history"]:
        dup = con.execute(
            f"SELECT pk_key, COUNT(*) c FROM {t}_current GROUP BY pk_key HAVING c > 1"
        ).fetchall()
        if dup:
            violations.append(f"{t}_current: duplicate pk_key(s) {dup}")
    return violations


def check_referential_integrity(con) -> list[str]:
    violations = []
    wallets = {pk: row for pk, row in _rows(con, "wallets")}
    customer_ids = {row["customer_id"] for _, row in _rows(con, "customers")}
    for pk, row in wallets.items():
        if row["customer_id"] not in customer_ids:
            violations.append(f"wallets_current {pk}: customer_id {row['customer_id']} not found in customers")

    wallet_ids = set(wallets.keys())
    for pk, row in _rows(con, "transactions"):
        for side in ("source_wallet_id", "destination_wallet_id"):
            v = row.get(side)
            if v is not None and str(v) not in wallet_ids:
                violations.append(f"transactions_current {pk}: {side}={v} not found in wallets")
    return violations


def check_non_negative_balance(con) -> list[str]:
    violations = []
    for pk, row in _rows(con, "wallets"):
        if row["balance"] < 0:
            violations.append(f"wallets_current {pk}: negative balance {row['balance']}")
    return violations


def check_positive_transaction_amount(con) -> list[str]:
    violations = []
    for pk, row in _rows(con, "transactions"):
        if row["amount"] <= 0:
            violations.append(f"transactions_current {pk}: non-positive amount {row['amount']}")
    return violations


def check_line_items_sum_matches_transaction(con) -> list[str]:
    violations = []
    txns = {row["transaction_id"]: row for _, row in _rows(con, "transactions")}
    sums = {}
    for _, row in _rows(con, "transaction_line_items"):
        sums.setdefault(row["transaction_id"], 0.0)
        sums[row["transaction_id"]] += row["amount"]
    for txn_id, total in sums.items():
        expected = txns.get(txn_id, {}).get("amount")
        if expected is not None and round(total, 2) != round(expected, 2):
            violations.append(
                f"transaction {txn_id}: line items sum {total} != transaction amount {expected}"
            )
    return violations


def check_settled_after_initiated(con) -> list[str]:
    violations = []
    for pk, row in _rows(con, "transactions"):
        if row.get("settled_at") and row["settled_at"] < row["initiated_at"]:
            violations.append(f"transactions_current {pk}: settled_at before initiated_at")
    return violations


ALL_CHECKS = [
    check_pk_uniqueness,
    check_referential_integrity,
    check_non_negative_balance,
    check_positive_transaction_amount,
    check_line_items_sum_matches_transaction,
    check_settled_after_initiated,
]


def run_all(db_path=WAREHOUSE_PATH) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    report = {}
    for check in ALL_CHECKS:
        report[check.__name__] = check(con)
    con.close()
    return report


if __name__ == "__main__":
    report = run_all()
    failed = {k: v for k, v in report.items() if v}
    if failed:
        print("VALIDATION FAILURES:")
        for k, v in failed.items():
            print(f"  {k}: {v}")
        raise SystemExit(1)
    print("All validations passed.")
