"""Validation-parity rules S1…S14 (SCHEMA.md §6), defined once and keyed by rule id.

Every rule builds SQL queries over the warehouse. Each query returns
``(seq, pk, reason)`` for a violation. Queries are scoped to the current batch through
the temp table ``_qb`` (the batch's events; the whole lake for a full verification), so a
long-standing anomaly is reported once, when its row changes, not on every batch.

Column-level rules (NOT NULL, domains, ranges) and the row predicates (``table_checks``)
come from the contract — the same definitions that generate the SQLite CHECKs.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from pipeline.contract import Contract
from pipeline.warehouse.model import WarehouseModel

BLOCK = "block"
QUARANTINE = "quarantine"
IN_BATCH = "c._last_seq IN (SELECT seq FROM _qb)"


@dataclass(frozen=True)
class Ctx:
    model: WarehouseModel
    contract: Contract


@dataclass(frozen=True)
class Rule:
    id: str
    description: str
    severity: str
    build: Callable[[Ctx], list[tuple[str, str]]]  # -> [(table_name, sql)]


# ------------------------------------------------------------------ column rules from the contract


def column_rule_specs(contract: Contract) -> list[tuple[str, str, str, str, object]]:
    """``(rule_id, table, column, kind, value)`` for every contract column rule."""
    specs = []
    for tname, table in contract.tables.items():
        for col in table.columns:
            if not col.nullable:
                specs.append(("S1", tname, col.name, "not_null", True))
            if col.enum:
                specs.append(("S1", tname, col.name, "enum", tuple(col.enum)))
            if col.min is not None:
                specs.append((col.range_rule, tname, col.name, "min", col.min))
            if col.max is not None:
                specs.append((col.range_rule, tname, col.name, "max", col.max))
    return specs


def _pk_json(tm, alias: str) -> str:
    pairs = ", ".join(
        f"'{src}', {alias}.{name}"
        for src, name in zip(tm.source_pk, tm.pk, strict=True)
    )
    return f"CAST(json_object({pairs}) AS VARCHAR)"


def _wh(tm, source: str):
    for col in tm.columns:
        if col.source == source and col.until is None:
            return col
    raise KeyError(source)


def _scoped(tm, cur: str, col, where: str, reason: str) -> str:
    return (
        f"SELECT c._last_seq AS seq, {_pk_json(tm, 'c')} AS pk, {reason} AS reason "
        f"FROM {cur} c WHERE NOT c._is_deleted AND {IN_BATCH} "
        f"AND c._schema_version >= {col.since} AND ({where})"
    )


def _threshold(col, value) -> str:
    return str(Decimal(value) / 100) if col.kind == "money" else str(value)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _translate(tm, expr: str) -> str:
    """Rewrite a SQLite row predicate for the warehouse column names."""
    renames = {
        c.source: c.name for c in tm.columns if c.until is None and c.source != c.name
    }
    for source in sorted(renames, key=len, reverse=True):
        expr = re.sub(rf"\b{re.escape(source)}\b", renames[source], expr)
    return expr


def _s1(ctx: Ctx):
    out = []
    for tname, table in ctx.contract.tables.items():
        tm = ctx.model.tables[tname]
        cur = ctx.model.current_name(tname)
        pk = ", ".join(tm.pk)
        out.append(
            (
                tname,
                (
                    f"SELECT max(c._last_seq) AS seq, {_pk_json(tm, 'c')} AS pk, "
                    f"'duplicate primary key' AS reason FROM "
                    f"(SELECT *, count(*) OVER (PARTITION BY {pk}) AS _n FROM {cur} "
                    f"WHERE NOT _is_deleted) c WHERE c._n > 1 AND {IN_BATCH} "
                    f"GROUP BY {', '.join('c.' + n for n in tm.pk)}"
                ),
            )
        )
        for rule, t, column, kind, value in column_rule_specs(ctx.contract):
            if rule != "S1" or t != tname:
                continue
            col = _wh(tm, column)
            if kind == "not_null":
                out.append(
                    (
                        tname,
                        _scoped(
                            tm,
                            cur,
                            col,
                            f"c.{col.name} IS NULL",
                            _literal(f"{tname}.{column} is NULL"),
                        ),
                    )
                )
            else:
                values = ", ".join(_literal(v) for v in value)
                out.append(
                    (
                        tname,
                        _scoped(
                            tm,
                            cur,
                            col,
                            f"c.{col.name} IS NOT NULL AND c.{col.name} NOT IN ({values})",
                            f"'{tname}.{column} has value ' || c.{col.name} || ' outside its domain'",
                        ),
                    )
                )
        for fk in table.foreign_keys:
            parent = ctx.model.tables[fk.references]
            fcols = [_wh(tm, c).name for c in fk.columns]
            pcols = [_wh(parent, c).name for c in fk.ref_columns]
            present = " AND ".join(f"c.{c} IS NOT NULL" for c in fcols)
            match = " AND ".join(
                f"p.{p} = c.{f}" for p, f in zip(pcols, fcols, strict=True)
            )
            out.append(
                (
                    tname,
                    (
                        f"SELECT c._last_seq AS seq, {_pk_json(tm, 'c')} AS pk, "
                        f"'foreign key ({', '.join(fk.columns)}) -> {fk.references} not found' AS reason "
                        f"FROM {cur} c WHERE NOT c._is_deleted AND {IN_BATCH} AND {present} "
                        f"AND NOT EXISTS (SELECT 1 FROM {ctx.model.current_name(fk.references)} p "
                        f"WHERE NOT p._is_deleted AND {match})"
                    ),
                )
            )
    return out


def _range_queries(ctx: Ctx, rule_id: str):
    out = []
    for rule, tname, column, kind, value in column_rule_specs(ctx.contract):
        if rule != rule_id or kind not in ("min", "max"):
            continue
        tm = ctx.model.tables[tname]
        col = _wh(tm, column)
        op = "<" if kind == "min" else ">"
        text = _threshold(col, value)
        out.append(
            (
                tname,
                _scoped(
                    tm,
                    ctx.model.current_name(tname),
                    col,
                    f"c.{col.name} {op} {text}",
                    f"'{tname}.{column} = ' || c.{col.name} || ' violates {kind} {text}'",
                ),
            )
        )
    return out


def _table_checks(ctx: Ctx, rule_id: str):
    out = []
    for tname, table in ctx.contract.tables.items():
        tm = ctx.model.tables[tname]
        for check in table.table_checks:
            if check.rule != rule_id:
                continue
            expr = _translate(tm, check.expr)
            out.append(
                (
                    tname,
                    (
                        f"SELECT c._last_seq AS seq, {_pk_json(tm, 'c')} AS pk, "
                        f"{_literal('row rule violated: ' + check.expr)} AS reason "
                        f"FROM {ctx.model.current_name(tname)} c "
                        f"WHERE NOT c._is_deleted AND {IN_BATCH} AND (({expr}) IS FALSE)"
                    ),
                )
            )
    return out


def _s2(ctx):
    return _range_queries(ctx, "S2")


def _s3(ctx):
    return _table_checks(ctx, "S3")


def _s4(ctx):
    return _range_queries(ctx, "S4") + _table_checks(ctx, "S4")


def _s6(ctx):
    return _table_checks(ctx, "S6")


def _s7(ctx):
    return _table_checks(ctx, "S7")


def _s5(ctx: Ctx):
    out = []
    for tname, table in ctx.contract.tables.items():
        if not table.transition_column:
            continue
        tm = ctx.model.tables[tname]
        col = _wh(tm, table.transition_column)
        pairs = ", ".join(
            f"({_literal(src)}, {_literal(dst)})"
            for src, dsts in table.transitions.items()
            for dst in dsts
        )
        pk = ", ".join(tm.pk)
        out.append(
            (
                tname,
                (
                    f"WITH v AS (SELECT *, lag({col.name}) OVER (PARTITION BY {pk} ORDER BY seq) AS _prev "
                    f"FROM {ctx.model.history_name(tname)} WHERE op <> 'delete') "
                    f"SELECT c.seq AS seq, {_pk_json(tm, 'c')} AS pk, "
                    f"'illegal transition ' || c._prev || ' -> ' || c.{col.name} AS reason "
                    f"FROM v c WHERE c.seq IN (SELECT seq FROM _qb) AND c._prev IS NOT NULL "
                    f"AND c._prev <> c.{col.name} AND NOT EXISTS "
                    f"(SELECT 1 FROM (VALUES {pairs}) t(a, b) WHERE t.a = c._prev AND t.b = c.{col.name})"
                ),
            )
        )
    return out


def _s8(ctx: Ctx):
    return [
        (
            t,
            (
                f"SELECT seq, pk, 'append-only table received an ' || op AS reason FROM _qb "
                f"WHERE table_name = '{t}' AND op IN ('update', 'delete')"
            ),
        )
        for t, table in ctx.contract.tables.items()
        if table.append_only
    ]


def _s14(ctx: Ctx):
    out = []
    for tname, table in ctx.contract.tables.items():
        changed = " OR ".join(
            f"json_extract_string(\"before\", '$.{c}') IS DISTINCT FROM "
            f"json_extract_string(\"after\", '$.{c}')"
            for c in table.primary_key
        )
        out.append(
            (
                tname,
                (
                    f"SELECT seq, pk, 'primary key changed' AS reason FROM _qb "
                    f"WHERE table_name = '{tname}' AND op = 'update' AND ({changed})"
                ),
            )
        )
        if table.no_delete:
            out.append(
                (
                    tname,
                    (
                        f"SELECT seq, pk, '{tname} rows must never be deleted' AS reason FROM _qb "
                        f"WHERE table_name = '{tname}' AND op = 'delete'"
                    ),
                )
            )
    return out


# Cross-row business rules (S9–S13). Entities touched by the batch are re-checked.

_BATCH = "IN (SELECT seq FROM _qb)"


def _s9(ctx: Ctx):
    return [
        (
            "orders",
            f"""
        WITH ev AS (
            SELECT order_id, _last_seq AS seq FROM warehouse.orders_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT order_id, _last_seq FROM warehouse.order_items_current WHERE _last_seq {_BATCH}),
        touched AS (SELECT order_id, max(seq) AS seq FROM ev GROUP BY order_id),
        sums AS (SELECT order_id, sum(line_total) AS s FROM warehouse.order_items_current
                 WHERE NOT _is_deleted GROUP BY order_id)
        SELECT t.seq AS seq, CAST(json_object('order_id', o.order_id) AS VARCHAR) AS pk,
               'order total ' || o.total || ' <> sum of lines ' || coalesce(s.s, 0) AS reason
        FROM touched t
        JOIN warehouse.orders_current o ON o.order_id = t.order_id AND NOT o._is_deleted
        LEFT JOIN sums s ON s.order_id = t.order_id
        WHERE o.total <> coalesce(s.s, 0)""",
        )
    ]


def _s10(ctx: Ctx):
    return [
        (
            "orders",
            f"""
        WITH ev AS (
            SELECT order_id, _last_seq AS seq FROM warehouse.orders_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT order_id, _last_seq FROM warehouse.payments_current WHERE _last_seq {_BATCH}),
        touched AS (SELECT order_id, max(seq) AS seq FROM ev GROUP BY order_id),
        sums AS (SELECT order_id, sum(amount) AS s FROM warehouse.payments_current
                 WHERE NOT _is_deleted AND status = 'success' GROUP BY order_id)
        SELECT t.seq AS seq, CAST(json_object('order_id', o.order_id) AS VARCHAR) AS pk,
               'successful payments ' || coalesce(s.s, 0) || ' <> order total ' || o.total AS reason
        FROM touched t
        JOIN warehouse.orders_current o ON o.order_id = t.order_id AND NOT o._is_deleted
        LEFT JOIN sums s ON s.order_id = t.order_id
        WHERE o.status IN ('paid', 'shipped', 'delivered', 'partially_refunded', 'refunded')
          AND o.total <> coalesce(s.s, 0)""",
        )
    ]


def _s11(ctx: Ctx):
    live = "NOT _is_deleted AND status <> 'rejected'"
    return [
        (
            "refunds",
            f"""
        WITH ev AS (
            SELECT order_id, line_no, _last_seq AS seq FROM warehouse.refunds_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT order_id, line_no, _last_seq FROM warehouse.order_items_current WHERE _last_seq {_BATCH}),
        touched AS (SELECT order_id, line_no, max(seq) AS seq FROM ev GROUP BY order_id, line_no),
        sums AS (SELECT order_id, line_no, sum(quantity) AS q FROM warehouse.refunds_current
                 WHERE {live} GROUP BY order_id, line_no)
        SELECT t.seq AS seq,
               CAST(json_object('order_id', t.order_id, 'line_no', t.line_no) AS VARCHAR) AS pk,
               'refunded quantity ' || s.q || ' exceeds line quantity ' || i.quantity AS reason
        FROM touched t
        JOIN warehouse.order_items_current i
          ON i.order_id = t.order_id AND i.line_no = t.line_no AND NOT i._is_deleted
        JOIN sums s ON s.order_id = t.order_id AND s.line_no = t.line_no
        WHERE s.q > i.quantity""",
        ),
        (
            "refunds",
            f"""
        WITH ev AS (
            SELECT payment_id, _last_seq AS seq FROM warehouse.refunds_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT payment_id, _last_seq FROM warehouse.payments_current WHERE _last_seq {_BATCH}),
        touched AS (SELECT payment_id, max(seq) AS seq FROM ev GROUP BY payment_id),
        sums AS (SELECT payment_id, sum(amount) AS a FROM warehouse.refunds_current
                 WHERE {live} GROUP BY payment_id)
        SELECT t.seq AS seq, CAST(json_object('payment_id', t.payment_id) AS VARCHAR) AS pk,
               'refunded amount ' || s.a || ' exceeds payment ' || p.amount AS reason
        FROM touched t
        JOIN warehouse.payments_current p ON p.payment_id = t.payment_id AND NOT p._is_deleted
        JOIN sums s ON s.payment_id = t.payment_id
        WHERE s.a > p.amount""",
        ),
    ]


def _s12(ctx: Ctx):
    return [
        (
            "wallets",
            f"""
        WITH ev AS (
            SELECT wallet_id, _last_seq AS seq FROM warehouse.wallets_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT wallet_id, _last_seq FROM warehouse.wallet_transactions_current WHERE _last_seq {_BATCH}),
        touched AS (SELECT wallet_id, max(seq) AS seq FROM ev GROUP BY wallet_id),
        ledger AS (SELECT wallet_id,
                          sum(CASE WHEN type = 'payment_debit' THEN -amount ELSE amount END) AS net,
                          arg_max(balance_after, _last_seq) AS latest
                   FROM warehouse.wallet_transactions_current WHERE NOT _is_deleted GROUP BY wallet_id)
        SELECT t.seq AS seq, CAST(json_object('wallet_id', w.wallet_id) AS VARCHAR) AS pk,
               'balance ' || w.balance || ' <> ledger ' || coalesce(l.net, 0)
               || ' (latest ' || coalesce(l.latest, 0) || ')' AS reason
        FROM touched t
        JOIN warehouse.wallets_current w ON w.wallet_id = t.wallet_id AND NOT w._is_deleted
        LEFT JOIN ledger l ON l.wallet_id = t.wallet_id
        WHERE w.balance <> coalesce(l.net, 0) OR w.balance <> coalesce(l.latest, 0)""",
        )
    ]


def _s13(ctx: Ctx):
    return [
        (
            "inventory",
            f"""
        WITH ev AS (
            SELECT product_id, _last_seq AS seq FROM warehouse.inventory_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT product_id, _last_seq FROM warehouse.order_items_current WHERE _last_seq {_BATCH}
            UNION ALL
            SELECT i.product_id, o._last_seq FROM warehouse.orders_current o
              JOIN warehouse.order_items_current i ON i.order_id = o.order_id
              WHERE o._last_seq {_BATCH}),
        touched AS (SELECT product_id, max(seq) AS seq FROM ev GROUP BY product_id),
        open_qty AS (SELECT i.product_id, sum(i.quantity) AS q
                     FROM warehouse.order_items_current i
                     JOIN warehouse.orders_current o ON o.order_id = i.order_id AND NOT o._is_deleted
                     WHERE NOT i._is_deleted AND o.status IN ('pending', 'paid')
                     GROUP BY i.product_id)
        SELECT t.seq AS seq, CAST(json_object('product_id', inv.product_id) AS VARCHAR) AS pk,
               'reserved ' || inv.reserved || ' <> open-order quantity ' || coalesce(q.q, 0) AS reason
        FROM touched t
        JOIN warehouse.inventory_current inv ON inv.product_id = t.product_id AND NOT inv._is_deleted
        LEFT JOIN open_qty q ON q.product_id = t.product_id
        WHERE inv.reserved <> coalesce(q.q, 0)""",
        )
    ]


RULES: dict[str, Rule] = {
    r.id: r
    for r in [
        Rule(
            "S1",
            "Keys unique, foreign keys valid, NOT NULL set, enum values in domain",
            BLOCK,
            _s1,
        ),
        Rule("S2", "Amounts positive; balances and totals non-negative", BLOCK, _s2),
        Rule("S3", "line_total = quantity x unit_price", BLOCK, _s3),
        Rule("S4", "0 <= reserved <= on_hand", BLOCK, _s4),
        Rule("S5", "Only allowed status transitions", QUARANTINE, _s5),
        Rule("S6", "Event timestamps not before creation", BLOCK, _s6),
        Rule(
            "S7",
            "Payment method consistency (wallet_txn_id xor provider_ref)",
            BLOCK,
            _s7,
        ),
        Rule("S8", "Wallet ledger is append-only", BLOCK, _s8),
        Rule("S9", "Order total = sum of its lines", QUARANTINE, _s9),
        Rule(
            "S10", "Paid orders: sum of successful payments = total", QUARANTINE, _s10
        ),
        Rule(
            "S11", "Refunds within line quantity and payment amount", QUARANTINE, _s11
        ),
        Rule("S12", "Wallet balance = ledger", QUARANTINE, _s12),
        Rule("S13", "Reserved stock = quantity on open orders", QUARANTINE, _s13),
        Rule(
            "S14",
            "Primary keys immutable; users and wallets never deleted",
            BLOCK,
            _s14,
        ),
    ]
}
