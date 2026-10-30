"""Shared fixtures (TEST_PLAN.md §2)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from demo import seed_basic
from pipeline.clock import FakeClock
from pipeline.contract import load_contract
from source.app import ShopApp
from source.db import connect, init_source

CONTRACT_PATH = (
    Path(__file__).resolve().parent.parent / "contracts" / "source_contract.yml"
)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def contract():
    return load_contract(CONTRACT_PATH)


class Source:
    """A bare source database (no pipeline) for Phase 1 tests."""

    def __init__(self, path, contract, clock):
        self.conn = connect(str(path))
        init_source(self.conn, contract)
        self.app = ShopApp(self.conn, clock)

    def events(self, after_seq: int = 0) -> list[dict]:
        rows = self.conn.execute(
            'SELECT seq, txn_id, schema_version, table_name, op, pk, "before", "after", '
            "changed_at FROM change_log WHERE seq > ? ORDER BY seq",
            (after_seq,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for key in ("pk", "before", "after"):
                d[key] = json.loads(d[key]) if d[key] is not None else None
            out.append(d)
        return out

    def max_seq(self) -> int:
        return self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM change_log"
        ).fetchone()[0]

    def row(self, table: str, **where) -> dict:
        cond = " AND ".join(f"{k} = ?" for k in where)
        r = self.conn.execute(
            f"SELECT * FROM {table} WHERE {cond}", list(where.values())
        ).fetchone()
        return dict(r) if r else {}

    def seed(self):
        return seed_basic(self.app)


@pytest.fixture
def source(tmp_path, contract, clock):
    src = Source(tmp_path / "source.db", contract, clock)
    yield src
    src.conn.close()


@pytest.fixture
def seeded(source):
    return source, source.seed()


def expect_rejected(conn: sqlite3.Connection, sql: str, params=()):
    """Assert a write is refused by the database (constraint or trigger)."""
    with pytest.raises(sqlite3.DatabaseError):
        conn.execute(sql, params)


# ---------------------------------------------------------------------- platform fixtures

from datetime import UTC, datetime

from demo import Workbench
from pipeline.cdc.reader import Event


@pytest.fixture
def platform(tmp_path, clock):
    """Source + pipeline + DuckDB in tmp_path (TEST_PLAN.md §2)."""
    wb = Workbench(tmp_path, clock=clock)
    yield wb
    wb.close()


@pytest.fixture
def seeded_platform(platform):
    """``seed_basic()``: users, wallets with top-ups, products with inventory."""
    return platform, seed_basic(platform.app)


@pytest.fixture
def make_workbench(tmp_path, clock):
    """Factory for extra workbenches (custom config / fault injection)."""
    made = []

    def build(name="wb", **kwargs):
        wb = Workbench(tmp_path / name, clock=kwargs.pop("clock", clock), **kwargs)
        made.append(wb)
        return wb

    yield build
    for wb in made:
        wb.close()


@pytest.fixture
def fault_after(platform):
    """``fault_after(n)``: a pipeline that crashes after n events inside a batch."""

    def build(n):
        return platform.new_pipeline(fault_after=n)

    return build


AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def make_event(seq, table, op, pk, before=None, after=None, at=AT, version=1, txn=None):
    """Hand-made change event for ``inject_events``."""
    return Event(
        seq=seq,
        txn_id=txn or f"X-{seq}",
        schema_version=version,
        table_name=table,
        op=op,
        pk=pk,
        before=before,
        after=after,
        changed_at=at,
    )


@pytest.fixture
def inject_events(platform):
    """``inject_events(events)``: feed events to the pipeline, bypassing the source."""

    def inject(events, pipeline=None, advance_checkpoint=False):
        (pipeline or platform.pipeline).process_events(list(events), advance_checkpoint)

    return inject


def mixed_workload(wb, step: int = 30):
    """Deterministic workload touching all 9 tables; time advances ``step`` s between helpers.

    Returns ``(ids, marks)`` where ``marks`` is a list of ``(clock time, source snapshot)``
    taken after every helper call, for point-in-time comparisons.
    """
    app, clock = wb.app, wb.clock
    marks = []

    def mark():
        marks.append((clock.now(), wb.snapshot_source()))
        clock.advance(step)

    ids = seed_basic(app)
    mark()
    o1 = app.place_order(ids["u1"], [(ids["p1"], 2), (ids["p2"], 1)])
    mark()
    app.pay_order(
        o1, [("upi", 10_00, False), ("wallet", 200_00, True), ("card", 190_00, True)]
    )
    mark()
    app.ship_order(o1)
    mark()
    app.deliver_order(o1)
    mark()
    pays = {
        r["method"]: r["payment_id"]
        for r in wb.src.execute(
            "SELECT * FROM payments WHERE order_id = ? AND status = 'success'", (o1,)
        )
    }
    r1 = app.request_refund(o1, 1, pays["wallet"], 1, 100_00, "damaged")
    app.approve_refund(r1)
    mark()
    app.process_refund(r1)
    mark()
    r2 = app.request_refund(o1, 2, pays["card"], 1, 90_00, "not_needed")
    app.reject_refund(r2)
    mark()
    r3 = app.request_refund(o1, 2, pays["card"], 1, 90_00, "wrong_item")
    app.approve_refund(r3)
    app.process_refund(r3)
    mark()
    o2 = app.place_order(ids["u2"], [(ids["p3"], 1)])
    mark()
    app.cancel_order(o2)
    mark()
    o3 = app.place_order(ids["u2"], [(ids["p1"], 1), (ids["p3"], 1)])
    app.remove_order_line(o3, 2)
    mark()
    app.restock(ids["p3"], 4)
    app.update_product_price(ids["p3"], 45_00)
    mark()
    extra = app.create_product("SKU-9", "Temp", 10_00, None, 1)
    app.delete_product(extra)
    app.discontinue_product(ids["p2"])
    mark()
    ids.update(o1=o1, o2=o2, o3=o3, r1=r1, r3=r3)
    return ids, marks
