#!/usr/bin/env python3
"""
demo_time_travel.py

Demonstrates the warehouse's point-in-time reconstruction feature.

Scenario
--------
  t0  : insert wallet w_demo at balance 100
  t1  : update balance to 60
  t2  : update balance to 0 (closed)

The script then asks the warehouse what the wallet looked like at each
of those times and prints the answer. Reads come from
`wh_wallets_history` only — the lake is not touched.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from pipeline.cdc import CDCCapture
from pipeline.orchestrator import bootstrap, run_tick
from pipeline.warehouse import state_at
from source.models import create_source_tables


def _t(seconds: int) -> datetime:
    base = datetime(2026, 6, 1, 9, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds)


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)
    bootstrap(conn)
    capture = CDCCapture()

    # Customer must exist for FK.
    cust_row = {
        "customer_id":       "c_demo",
        "full_name":         "Demo Customer",
        "email":             "demo@example.invalid",
        "country_code":      "US",
        "status":            "active",
        "registration_date": _t(0).date(),
        "created_at":        _t(0),
        "updated_at":        _t(0),
    }
    capture.insert("customers", "c_demo", cust_row, captured_at=_t(0))

    # Three revisions of one wallet:
    w0 = {
        "wallet_id":   "w_demo",
        "customer_id": "c_demo",
        "currency":    "USD",
        "balance":     Decimal("100.0000"),
        "status":      "active",
        "created_at":  _t(0),
        "updated_at":  _t(0),
    }
    capture.insert("wallets", "w_demo", w0, captured_at=_t(0))

    w1 = dict(w0, balance=Decimal("60.0000"), updated_at=_t(60))
    capture.update("wallets", "w_demo", before=w0, after=w1, captured_at=_t(60))

    w2 = dict(w1, balance=Decimal("0.0000"), status="closed", updated_at=_t(120))
    capture.update("wallets", "w_demo", before=w1, after=w2, captured_at=_t(120))

    run_tick(conn, capture)

    print("== state_at queries ==")
    for label, ts in [
        ("at t0  (just after insert)", _t(0)),
        ("at t30 (between insert and first update)", _t(30)),
        ("at t60 (just after first update)", _t(60)),
        ("at t90 (between updates)", _t(90)),
        ("at t150 (after close)", _t(150)),
    ]:
        rows = state_at(conn, "wallets", ts)
        balance = rows[0]["balance"] if rows else None
        status = rows[0]["status"] if rows else None
        print(f"  {label}: balance={balance}, status={status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
