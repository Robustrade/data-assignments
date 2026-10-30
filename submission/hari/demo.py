"""End-to-end walkthrough and the shared test/CI workbench.

``python demo.py`` builds everything from nothing in ``./data/`` with a fake clock and
fixed ids, so two runs print identical output (OPS-06).
"""

from __future__ import annotations

import hashlib
import shutil
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.cdc.runner import Pipeline
from pipeline.clock import FakeClock, parse_iso
from pipeline.config import Config, load_config
from pipeline.contract import load_contract
from source.app import ShopApp
from source.db import connect, init_source


def seed_basic(app: ShopApp) -> dict[str, str]:
    """Two users with funded wallets and three stocked products (TEST_PLAN.md §2)."""
    u1 = app.create_user("Asha", "asha@example.com", "+911111111111")
    u2 = app.create_user("Ravi", "ravi@example.com")
    app.topup_wallet(u1, 500_00)
    app.topup_wallet(u2, 200_00)
    p1 = app.create_product("SKU-1", "Kettle", 150_00, "kitchen", 10)
    p2 = app.create_product("SKU-2", "Toaster", 90_00, "kitchen", 5)
    p3 = app.create_product("SKU-3", "Lamp", 40_00, "home", 3)
    return {"u1": u1, "u2": u2, "p1": p1, "p2": p2, "p3": p3}


class Workbench:
    """A source database + pipeline + fake clock in one folder."""

    def __init__(
        self, directory, clock=None, fault_after=None, owner="pipeline", **overrides
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.clock = clock or FakeClock()
        self.config: Config = load_config(
            env={},
            source_db=self.directory / "source.db",
            platform_db=self.directory / "platform.duckdb",
            **overrides,
        )
        self.contract = load_contract(self.config.contract_path)
        self.src = connect(str(self.config.source_db))
        init_source(self.src, self.contract)
        self.app = ShopApp(self.src, self.clock)
        self.pipeline = self.new_pipeline(fault_after=fault_after, owner=owner)
        self.pipeline.init()

    # ------------------------------------------------------------------ construction

    def new_pipeline(self, fault_after=None, owner="pipeline", config=None) -> Pipeline:
        return Pipeline(
            config or self.config, self.clock, owner=owner, fault_after=fault_after
        )

    def restart(self, fault_after=None) -> Pipeline:
        """Simulate a process restart: new pipeline object on the same files."""
        self.pipeline.close()
        self.pipeline = self.new_pipeline(fault_after=fault_after)
        self.pipeline.init()
        return self.pipeline

    @property
    def conn(self):
        return self.pipeline.conn

    def sync(self):
        """Drain the whole backlog into the lake and warehouse."""
        return self.pipeline.run_until_caught_up()

    # ------------------------------------------------------------------ reads

    def snapshot_source(self, contract=None) -> dict[str, list[dict]]:
        """All source tables, normalised like the warehouse (rupees, timestamps, hashes)."""
        contract = contract or self.contract
        out: dict[str, list[dict]] = {}
        for name, table in contract.tables.items():
            order = ", ".join(table.primary_key)
            rows = []
            for raw in self.src.execute(f"SELECT * FROM {name} ORDER BY {order}"):
                row = {}
                for col in table.columns:
                    value = raw[col.name]
                    if col.name in table.pii:
                        row[f"{col.name}_hash"] = (
                            None
                            if value is None
                            else hashlib.sha256(
                                value.strip().lower().encode()
                            ).hexdigest()
                        )
                    elif col.money:
                        key = col.name.removesuffix("_paise")
                        row[key] = None if value is None else Decimal(value) / 100
                    elif col.timestamp:
                        row[col.name] = (
                            None
                            if value is None
                            else parse_iso(value).astimezone(UTC).replace(tzinfo=None)
                        )
                    else:
                        row[col.name] = value
                rows.append(row)
            out[name] = rows
        return out

    def view(self, table: str, conn=None) -> list[dict]:
        """Rows of the consumer view ``warehouse.<table>``, ordered by key."""
        conn = conn or self.conn
        order = ", ".join(self.contract.tables[table].primary_key)
        rel = conn.execute(f"SELECT * FROM warehouse.{table} ORDER BY {order}")
        names = [d[0] for d in rel.description]
        return [dict(zip(names, r, strict=True)) for r in rel.fetchall()]

    def warehouse(self, conn=None) -> dict[str, list[dict]]:
        return {t: self.view(t, conn) for t in self.contract.tables}

    def scalar(self, sql: str, params=None):
        return self.conn.execute(sql, params or []).fetchone()[0]

    def close(self) -> None:
        self.pipeline.close()
        self.src.close()


# ---------------------------------------------------------------------- the E2E-01 story


def _order_line(wb) -> str:
    rows = wb.conn.execute(
        "SELECT order_id, status, total FROM warehouse.orders ORDER BY order_id"
    ).fetchall()
    return "; ".join(f"{o} {s} total={t}" for o, s, t in rows) or "(none)"


def run_story(wb: Workbench, emit=print) -> list[tuple[str, datetime, dict]]:
    """Top up -> order 2 products -> split payment -> ship -> deliver -> partial refunds.

    After every step the pipeline catches up and a summary is emitted. Returns
    ``[(label, source commit time, source snapshot)]`` so callers can verify ``as_of``.
    """
    app, clock = wb.app, wb.clock
    marks: list[tuple[str, datetime, dict]] = []
    state = {"seq": 0}

    def step(title, action):
        clock.advance(60)
        action()
        added = wb.src.execute(
            "SELECT COUNT(*) FROM change_log WHERE seq > ?", (state["seq"],)
        ).fetchone()[0]
        state["seq"] = wb.src.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM change_log"
        ).fetchone()[0]
        wb.sync()
        emit(f"== {title}")
        emit(f"   change_log rows added: {added} (total {state['seq']})")
        emit(f"   lake events: {wb.scalar('SELECT COUNT(*) FROM lake.cdc_events')}")
        emit(f"   warehouse.orders: {_order_line(wb)}")
        balance = wb.scalar(
            "SELECT balance FROM warehouse.wallets ORDER BY wallet_id LIMIT 1"
        )
        emit(f"   warehouse.wallets (first): balance={balance}")
        marks.append((title, clock.now(), wb.snapshot_source()))

    ids: dict[str, str] = {}
    box: dict[str, str] = {}

    def setup():
        ids.update(seed_basic(app))

    step("1. create users, products, stock, wallet top-ups", setup)
    step("2. top up the wallet again", lambda: app.topup_wallet(ids["u1"], 50_00))

    def place():
        box["order"] = app.place_order(ids["u1"], [(ids["p1"], 1), (ids["p2"], 2)])

    step("3. place an order for 2 products (stock reserved)", place)

    def pay():
        app.pay_order(
            box["order"],
            [("upi", 50_00, False), ("wallet", 200_00, True), ("card", 130_00, True)],
        )

    step("4. pay: failed UPI attempt, then wallet + card", pay)
    step("5. ship", lambda: app.ship_order(box["order"]))
    step("6. deliver", lambda: app.deliver_order(box["order"]))

    def refunds():
        pays = {
            r["method"]: r["payment_id"]
            for r in wb.src.execute(
                "SELECT * FROM payments WHERE order_id = ? AND status = 'success'",
                (box["order"],),
            )
        }
        for line_no, method, amount, reason in (
            (1, "wallet", 100_00, "damaged"),
            (2, "card", 90_00, "not_needed"),
        ):
            refund = app.request_refund(
                box["order"], line_no, pays[method], 1, amount, reason
            )
            app.approve_refund(refund)
            app.process_refund(refund)

    step("7. partial refunds back to wallet and card", refunds)
    return marks


def run_demo(directory, emit=print) -> Workbench:
    """The deterministic walkthrough used by ``python demo.py`` and OPS-06."""
    from pipeline.errors import SchemaContractViolation

    wb = Workbench(directory, clock=FakeClock())
    emit("CDC lakehouse demo (fake clock, fixed ids)")
    marks = run_story(wb, emit)

    emit("== time travel: the order as_of each step")
    for label, moment, _ in marks[2:]:
        order = wb.pipeline.as_of(moment)["orders"][0]
        emit(
            f"   as_of {moment.strftime('%H:%M:%S')} -> {order['order_id']} {order['status']} ({label.split('. ')[1]})"
        )
    now_status = wb.view("orders")[0]["status"]
    emit(f"   now -> {now_status}")

    emit("== breaking schema change: users.phone dropped in the source")
    wb.src.execute("DROP TRIGGER cdc_users_ins")
    wb.src.execute("DROP TRIGGER cdc_users_upd")
    wb.src.execute("DROP TRIGGER cdc_users_del")
    wb.src.execute("ALTER TABLE users DROP COLUMN phone")
    wb.app.restock(
        wb.src.execute("SELECT product_id FROM products LIMIT 1").fetchone()[0], 1
    )
    try:
        wb.pipeline.run_once()
    except SchemaContractViolation as err:
        emit(f"   pipeline raised SchemaContractViolation: {err}")
    status = wb.pipeline.status()
    emit(f"   status={status['status']}")
    alert = wb.conn.execute(
        "SELECT level, reason FROM cdc.alerts ORDER BY alert_id DESC LIMIT 1"
    ).fetchone()
    emit(f"   alert: {alert[0]} - {alert[1]}")
    emit(f"   events held back in change_log: {wb.pipeline.run_once().lag_events}")

    emit("== data quality report (validation parity, all batches)")
    for rule, outcome, count in wb.conn.execute(
        "SELECT rule_id, status, count(*) FROM quality.check_results GROUP BY rule_id, status "
        "ORDER BY CAST(substr(rule_id, 2) AS INTEGER)"
    ).fetchall():
        emit(f"   {rule:<4} {outcome:<5} x{count}")
    emit(f"   quarantined rows: {wb.scalar('SELECT COUNT(*) FROM quality.quarantine')}")
    return wb


def main() -> int:
    data = ROOT / "data"
    for name in (
        "source.db",
        "source.db-wal",
        "source.db-shm",
        "platform.duckdb",
        "platform.duckdb.wal",
    ):
        (data / name).unlink(missing_ok=True)
    if data.exists() and not any(data.iterdir()):
        shutil.rmtree(data)
    wb = run_demo(data)
    wb.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
