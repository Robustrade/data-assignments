"""The application's write helpers.

``ShopApp`` is the only code that writes business data. Each public method is one
SQLite transaction and enforces the cross-row rules S9–S13 that the database cannot
express as constraints. The helpers know nothing about CDC except that they tag the
transaction (``_txn_context``) so triggers can group its events.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import ClassVar

from pipeline.clock import Clock


class ShopError(Exception):
    """A business rule was violated; the transaction was rolled back."""


class InsufficientStock(ShopError):
    pass


class InsufficientFunds(ShopError):
    pass


class ShopApp:
    PREFIXES: ClassVar[dict[str, str]] = {
        "user": "U",
        "wallet": "W",
        "product": "P",
        "order": "O",
        "payment": "PAY",
        "wallet_txn": "WT",
        "refund": "R",
        "provider": "PRV",
        "refund_provider": "RFP",
    }
    _SOURCES: ClassVar[dict[str, tuple[str, str]]] = {
        "user": ("users", "user_id"),
        "wallet": ("wallets", "wallet_id"),
        "product": ("products", "product_id"),
        "order": ("orders", "order_id"),
        "payment": ("payments", "payment_id"),
        "wallet_txn": ("wallet_transactions", "wallet_txn_id"),
        "refund": ("refunds", "refund_id"),
        "provider": ("payments", "provider_ref"),
        "refund_provider": ("refunds", "provider_ref"),
    }

    def __init__(self, conn: sqlite3.Connection, clock: Clock):
        self.conn = conn
        self.clock = clock
        self._counters: dict[str, int] = {}
        self._now = ""

    # ------------------------------------------------------------------ plumbing

    def _next_id(self, kind: str) -> str:
        prefix = self.PREFIXES[kind]
        if kind not in self._counters:
            table, column = self._SOURCES[kind]
            rows = self.conn.execute(
                f"SELECT {column} FROM {table} WHERE {column} LIKE ?", (prefix + "-%",)
            ).fetchall()
            numbers = [int(r[0].rsplit("-", 1)[1]) for r in rows]
            self._counters[kind] = max(numbers, default=0)
        self._counters[kind] += 1
        return f"{prefix}-{self._counters[kind]:04d}"

    @contextmanager
    def _txn(self):
        """One transaction: tag it for CDC, commit on success, roll back on error."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT COALESCE((SELECT seq FROM sqlite_sequence "
                "WHERE name = 'change_log'), 0) + 1"
            ).fetchone()
            self._now = self.clock.iso()
            self.conn.execute(
                'UPDATE _txn_context SET txn_id = ?, "now" = ?',
                (f"T-{row[0]:06d}", self._now),
            )
            yield
            self.conn.execute('UPDATE _txn_context SET txn_id = NULL, "now" = NULL')
            self.conn.execute("COMMIT")
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def _one(self, sql: str, params=()) -> sqlite3.Row:
        row = self.conn.execute(sql, params).fetchone()
        if row is None:
            raise ShopError(f"not found: {sql} {params}")
        return row

    def _ledger(self, wallet_id: str, kind: str, amount: int) -> str:
        """Append a ledger row and move the wallet balance (S12)."""
        wallet = self._one("SELECT * FROM wallets WHERE wallet_id = ?", (wallet_id,))
        if wallet["status"] != "active":
            raise ShopError(f"wallet {wallet_id} is {wallet['status']}")
        delta = -amount if kind == "payment_debit" else amount
        balance = wallet["balance_paise"] + delta
        if balance < 0:
            raise InsufficientFunds(f"wallet {wallet_id} has {wallet['balance_paise']}")
        txn_id = self._next_id("wallet_txn")
        self.conn.execute(
            "INSERT INTO wallet_transactions (wallet_txn_id, wallet_id, type, amount_paise, balance_after_paise, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (txn_id, wallet_id, kind, amount, balance, self._now),
        )
        self.conn.execute(
            "UPDATE wallets SET balance_paise = ?, updated_at = ? WHERE wallet_id = ?",
            (balance, self._now, wallet_id),
        )
        return txn_id

    def _wallet_of_user(self, user_id: str) -> str:
        return self._one("SELECT wallet_id FROM wallets WHERE user_id = ?", (user_id,))[
            0
        ]

    def _lines(self, order_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM order_items WHERE order_id = ? ORDER BY line_no", (order_id,)
        ).fetchall()

    def _adjust_inventory(self, product_id: str, on_hand: int, reserved: int) -> None:
        self.conn.execute(
            "UPDATE inventory SET on_hand = on_hand + ?, reserved = reserved + ?, "
            "updated_at = ? WHERE product_id = ?",
            (on_hand, reserved, self._now, product_id),
        )

    def _set_order_status(self, order_id: str, status: str, stamp: str | None) -> None:
        sets = "status = ?, updated_at = ?"
        params: list = [status, self._now]
        if stamp:
            sets += f", {stamp} = ?"
            params.append(self._now)
        self.conn.execute(
            f"UPDATE orders SET {sets} WHERE order_id = ?", (*params, order_id)
        )

    # ------------------------------------------------------------------ users & wallets

    def create_user(self, name: str, email: str, phone: str | None = None) -> str:
        with self._txn():
            user_id = self._next_id("user")
            self.conn.execute(
                "INSERT INTO users (user_id, name, email, phone, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'active', ?, ?)",
                (user_id, name, email, phone, self._now, self._now),
            )
            self.conn.execute(
                "INSERT INTO wallets (wallet_id, user_id, balance_paise, currency, status, created_at, updated_at) VALUES (?, ?, 0, 'INR', 'active', ?, ?)",
                (self._next_id("wallet"), user_id, self._now, self._now),
            )
        return user_id

    def close_user(self, user_id: str) -> None:
        with self._txn():
            open_orders = self._one(
                "SELECT COUNT(*) FROM orders WHERE user_id = ? "
                "AND status IN ('pending', 'paid', 'shipped')",
                (user_id,),
            )[0]
            if open_orders:
                raise ShopError(f"user {user_id} has open orders")
            self.conn.execute(
                "UPDATE users SET status = 'closed', updated_at = ? WHERE user_id = ?",
                (self._now, user_id),
            )
            self.conn.execute(
                "UPDATE wallets SET status = 'closed', updated_at = ? WHERE user_id = ?",
                (self._now, user_id),
            )

    def topup_wallet(self, user_id: str, amount_paise: int) -> str:
        with self._txn():
            return self._ledger(self._wallet_of_user(user_id), "topup", amount_paise)

    # ------------------------------------------------------------------ catalogue & stock

    def create_product(
        self,
        sku: str,
        name: str,
        price_paise: int,
        category: str | None = None,
        initial_stock: int = 0,
    ) -> str:
        with self._txn():
            product_id = self._next_id("product")
            self.conn.execute(
                "INSERT INTO products (product_id, sku, name, category, price_paise, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)",
                (product_id, sku, name, category, price_paise, self._now, self._now),
            )
            self.conn.execute(
                "INSERT INTO inventory (product_id, on_hand, reserved, updated_at) VALUES (?, ?, 0, ?)",
                (product_id, initial_stock, self._now),
            )
        return product_id

    def restock(self, product_id: str, quantity: int) -> None:
        if quantity <= 0:
            raise ShopError("restock quantity must be positive")
        with self._txn():
            self._one("SELECT 1 FROM inventory WHERE product_id = ?", (product_id,))
            self._adjust_inventory(product_id, quantity, 0)

    def discontinue_product(self, product_id: str) -> None:
        with self._txn():
            self.conn.execute(
                "UPDATE products SET status = 'discontinued', updated_at = ? "
                "WHERE product_id = ?",
                (self._now, product_id),
            )

    def delete_product(self, product_id: str) -> None:
        """Hard-delete a product that was never ordered (inventory first)."""
        with self._txn():
            ordered = self._one(
                "SELECT COUNT(*) FROM order_items WHERE product_id = ?", (product_id,)
            )[0]
            if ordered:
                raise ShopError(
                    f"product {product_id} was ordered; discontinue it instead"
                )
            self.conn.execute(
                "DELETE FROM inventory WHERE product_id = ?", (product_id,)
            )
            self.conn.execute(
                "DELETE FROM products WHERE product_id = ?", (product_id,)
            )

    def update_product_price(self, product_id: str, price_paise: int) -> None:
        with self._txn():
            self.conn.execute(
                "UPDATE products SET price_paise = ?, updated_at = ? WHERE product_id = ?",
                (price_paise, self._now, product_id),
            )

    # ------------------------------------------------------------------ orders

    def place_order(self, user_id: str, items: list[tuple[str, int]]) -> str:
        if not items:
            raise ShopError("an order needs at least one line")
        with self._txn():
            user = self._one("SELECT status FROM users WHERE user_id = ?", (user_id,))
            if user["status"] != "active":
                raise ShopError(f"user {user_id} is {user['status']}")
            wanted: dict[str, int] = {}
            lines = []
            for line_no, (product_id, qty) in enumerate(items, start=1):
                product = self._one(
                    "SELECT status, price_paise FROM products WHERE product_id = ?",
                    (product_id,),
                )
                if product["status"] != "active":
                    raise ShopError(f"product {product_id} is {product['status']}")
                wanted[product_id] = wanted.get(product_id, 0) + qty
                stock = self._one(
                    "SELECT on_hand - reserved FROM inventory WHERE product_id = ?",
                    (product_id,),
                )[0]
                if stock < wanted[product_id]:
                    raise InsufficientStock(
                        f"{product_id}: wanted {wanted[product_id]}, available {stock}"
                    )
                price = product["price_paise"]
                lines.append((line_no, product_id, qty, price, qty * price))
            order_id = self._next_id("order")
            total = sum(line[4] for line in lines)
            self.conn.execute(
                "INSERT INTO orders (order_id, user_id, status, total_paise, currency, placed_at, paid_at, shipped_at, delivered_at, cancelled_at, updated_at) VALUES (?, ?, 'pending', ?, 'INR', ?, NULL, NULL, NULL, NULL, ?)",
                (order_id, user_id, total, self._now, self._now),
            )
            for line_no, product_id, qty, price, line_total in lines:
                self.conn.execute(
                    "INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price_paise, line_total_paise) VALUES (?, ?, ?, ?, ?, ?)",
                    (order_id, line_no, product_id, qty, price, line_total),
                )
            for product_id, qty in wanted.items():
                self._adjust_inventory(product_id, 0, qty)
        return order_id

    def remove_order_line(self, order_id: str, line_no: int) -> None:
        with self._txn():
            order = self._one("SELECT * FROM orders WHERE order_id = ?", (order_id,))
            if order["status"] != "pending":
                raise ShopError("lines can only be removed from pending orders")
            lines = self._lines(order_id)
            if len(lines) <= 1:
                raise ShopError("cannot remove the last line; cancel the order")
            line = next((ln for ln in lines if ln["line_no"] == line_no), None)
            if line is None:
                raise ShopError(f"no line {line_no} on {order_id}")
            self.conn.execute(
                "DELETE FROM order_items WHERE order_id = ? AND line_no = ?",
                (order_id, line_no),
            )
            self.conn.execute(
                "UPDATE orders SET total_paise = ?, updated_at = ? WHERE order_id = ?",
                (order["total_paise"] - line["line_total_paise"], self._now, order_id),
            )
            self._adjust_inventory(line["product_id"], 0, -line["quantity"])

    def pay_order(self, order_id: str, attempts: list[tuple[str, int, bool]]) -> None:
        """Make payment attempts ``(method, amount_paise, succeed)`` in one transaction.

        Each attempt is one ``payments`` row. The order becomes ``paid`` once the
        successful payments add up to its total (S10); paying more than the total fails.
        """
        with self._txn():
            order = self._one("SELECT * FROM orders WHERE order_id = ?", (order_id,))
            if order["status"] != "pending":
                raise ShopError(f"order {order_id} is {order['status']}")
            paid = self._one(
                "SELECT COALESCE(SUM(amount_paise), 0) FROM payments "
                "WHERE order_id = ? AND status = 'success'",
                (order_id,),
            )[0]
            wallet_id = self._wallet_of_user(order["user_id"])
            for method, amount, succeed in attempts:
                payment_id = self._next_id("payment")
                self.conn.execute(
                    "INSERT INTO payments (payment_id, order_id, method, amount_paise, status, provider_ref, wallet_txn_id, created_at, completed_at) VALUES (?, ?, ?, ?, 'initiated', NULL, NULL, ?, NULL)",
                    (payment_id, order_id, method, amount, self._now),
                )
                if not succeed:
                    self.conn.execute(
                        "UPDATE payments SET status = 'failed', completed_at = ? "
                        "WHERE payment_id = ?",
                        (self._now, payment_id),
                    )
                    continue
                paid += amount
                if paid > order["total_paise"]:
                    raise ShopError(f"payments exceed the total of {order_id}")
                if method == "wallet":
                    ref = self._ledger(wallet_id, "payment_debit", amount)
                    self.conn.execute(
                        "UPDATE payments SET status = 'success', wallet_txn_id = ?, "
                        "completed_at = ? WHERE payment_id = ?",
                        (ref, self._now, payment_id),
                    )
                else:
                    self.conn.execute(
                        "UPDATE payments SET status = 'success', provider_ref = ?, "
                        "completed_at = ? WHERE payment_id = ?",
                        (self._next_id("provider"), self._now, payment_id),
                    )
            if paid == order["total_paise"]:
                self._set_order_status(order_id, "paid", "paid_at")
            else:
                self.conn.execute(
                    "UPDATE orders SET updated_at = ? WHERE order_id = ?",
                    (self._now, order_id),
                )

    def cancel_order(self, order_id: str) -> None:
        """Cancel a pending or paid order: release stock, return wallet payments.

        Card/UPI payments are reversed at the gateway, which is outside this model;
        only wallet money is moved back here (a ``refund_credit`` ledger row).
        """
        with self._txn():
            order = self._one("SELECT * FROM orders WHERE order_id = ?", (order_id,))
            if order["status"] not in ("pending", "paid"):
                raise ShopError(f"order {order_id} is {order['status']}")
            for line in self._lines(order_id):
                self._adjust_inventory(line["product_id"], 0, -line["quantity"])
            if order["status"] == "paid":
                wallet_id = self._wallet_of_user(order["user_id"])
                for pay in self.conn.execute(
                    "SELECT * FROM payments WHERE order_id = ? AND status = 'success' "
                    "AND method = 'wallet'",
                    (order_id,),
                ).fetchall():
                    self._ledger(wallet_id, "refund_credit", pay["amount_paise"])
            self._set_order_status(order_id, "cancelled", "cancelled_at")

    def ship_order(self, order_id: str) -> None:
        with self._txn():
            order = self._one(
                "SELECT status FROM orders WHERE order_id = ?", (order_id,)
            )
            if order["status"] != "paid":
                raise ShopError(f"order {order_id} is {order['status']}")
            for line in self._lines(order_id):
                self._adjust_inventory(
                    line["product_id"], -line["quantity"], -line["quantity"]
                )
            self._set_order_status(order_id, "shipped", "shipped_at")

    def deliver_order(self, order_id: str) -> None:
        with self._txn():
            order = self._one(
                "SELECT status FROM orders WHERE order_id = ?", (order_id,)
            )
            if order["status"] != "shipped":
                raise ShopError(f"order {order_id} is {order['status']}")
            self._set_order_status(order_id, "delivered", "delivered_at")

    # ------------------------------------------------------------------ refunds

    def request_refund(
        self,
        order_id: str,
        line_no: int,
        payment_id: str,
        quantity: int,
        amount_paise: int,
        reason: str,
    ) -> str:
        with self._txn():
            order = self._one(
                "SELECT status FROM orders WHERE order_id = ?", (order_id,)
            )
            if order["status"] not in ("delivered", "partially_refunded"):
                raise ShopError(f"order {order_id} is {order['status']}")
            line = self._one(
                "SELECT quantity FROM order_items WHERE order_id = ? AND line_no = ?",
                (order_id, line_no),
            )
            payment = self._one(
                "SELECT * FROM payments WHERE payment_id = ? AND order_id = ?",
                (payment_id, order_id),
            )
            if payment["status"] != "success":
                raise ShopError(f"payment {payment_id} was not successful")
            done_qty, done_amount = self._one(
                "SELECT COALESCE(SUM(CASE WHEN order_id = ? AND line_no = ? THEN quantity END), 0), "
                "COALESCE(SUM(CASE WHEN payment_id = ? THEN amount_paise END), 0) "
                "FROM refunds WHERE status <> 'rejected'",
                (order_id, line_no, payment_id),
            )
            if done_qty + quantity > line["quantity"]:
                raise ShopError("refund quantity exceeds the line quantity")
            if done_amount + amount_paise > payment["amount_paise"]:
                raise ShopError("refund amount exceeds the payment")
            refund_id = self._next_id("refund")
            self.conn.execute(
                "INSERT INTO refunds (refund_id, order_id, line_no, payment_id, quantity, amount_paise, reason, status, provider_ref, wallet_txn_id, created_at, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'requested', NULL, NULL, ?, NULL)",
                (
                    refund_id,
                    order_id,
                    line_no,
                    payment_id,
                    quantity,
                    amount_paise,
                    reason,
                    self._now,
                ),
            )
        return refund_id

    def _set_refund_status(self, refund_id: str, status: str) -> None:
        with self._txn():
            self.conn.execute(
                "UPDATE refunds SET status = ? WHERE refund_id = ?", (status, refund_id)
            )

    def approve_refund(self, refund_id: str) -> None:
        self._set_refund_status(refund_id, "approved")

    def reject_refund(self, refund_id: str) -> None:
        self._set_refund_status(refund_id, "rejected")

    def process_refund(self, refund_id: str) -> None:
        """Pay an approved refund back to the method of the original payment."""
        with self._txn():
            refund = self._one(
                "SELECT * FROM refunds WHERE refund_id = ?", (refund_id,)
            )
            if refund["status"] != "approved":
                raise ShopError(f"refund {refund_id} is {refund['status']}")
            payment = self._one(
                "SELECT * FROM payments WHERE payment_id = ?", (refund["payment_id"],)
            )
            order = self._one(
                "SELECT * FROM orders WHERE order_id = ?", (refund["order_id"],)
            )
            if payment["method"] == "wallet":
                wallet_id = self._wallet_of_user(order["user_id"])
                ref = self._ledger(wallet_id, "refund_credit", refund["amount_paise"])
                self.conn.execute(
                    "UPDATE refunds SET status = 'processed', wallet_txn_id = ?, "
                    "processed_at = ? WHERE refund_id = ?",
                    (ref, self._now, refund_id),
                )
            else:
                self.conn.execute(
                    "UPDATE refunds SET status = 'processed', provider_ref = ?, "
                    "processed_at = ? WHERE refund_id = ?",
                    (self._next_id("refund_provider"), self._now, refund_id),
                )
            refunded = self._one(
                "SELECT SUM(amount_paise) FROM refunds "
                "WHERE order_id = ? AND status = 'processed'",
                (refund["order_id"],),
            )[0]
            status = (
                "refunded" if refunded >= order["total_paise"] else "partially_refunded"
            )
            if order["status"] != status:
                self._set_order_status(refund["order_id"], status, None)
            else:
                self.conn.execute(
                    "UPDATE orders SET updated_at = ? WHERE order_id = ?",
                    (self._now, refund["order_id"]),
                )

    # ------------------------------------------------------------------ restore

    def restore_row(self, table: str, row: dict) -> None:
        """Write a row (e.g. from ``export_as_of``) back: update it, or insert if missing."""
        pk_cols = [
            r[1]
            for r in sorted(
                (r for r in self.conn.execute(f'PRAGMA table_info("{table}")') if r[5]),
                key=lambda r: r[5],
            )
        ]
        with self._txn():
            where = " AND ".join(f"{c} = ?" for c in pk_cols)
            exists = self.conn.execute(
                f"SELECT 1 FROM {table} WHERE {where}", [row[c] for c in pk_cols]
            ).fetchone()
            if exists:
                cols = [c for c in row if c not in pk_cols]
                assignments = ", ".join(f"{c} = ?" for c in cols)
                self.conn.execute(
                    f"UPDATE {table} SET {assignments} WHERE {where}",
                    [row[c] for c in cols] + [row[c] for c in pk_cols],
                )
            else:
                cols = list(row)
                marks = ", ".join("?" for _ in cols)
                self.conn.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({marks})",
                    [row[c] for c in cols],
                )
