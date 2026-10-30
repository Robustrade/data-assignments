"""Run the validation-parity rules inside the batch transaction."""

from __future__ import annotations

from datetime import datetime

from pipeline.cdc.reader import Event, dump_json
from pipeline.lake.writer import naive
from pipeline.quality.rules import BLOCK, QUARANTINE, RULES, Ctx


def _stage_batch(conn, events: list[Event] | None) -> None:
    """Create ``_qb``: the batch's events, or (``None``) every event in the lake."""
    if events is None:
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE _qb AS SELECT seq, txn_id, table_name, op, pk, "
            '"before", "after" FROM lake.cdc_events'
        )
        return
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _qb (seq BIGINT, txn_id VARCHAR, table_name VARCHAR, "
        'op VARCHAR, pk VARCHAR, "before" VARCHAR, "after" VARCHAR)'
    )
    conn.executemany(
        "INSERT INTO _qb VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            [
                e.seq,
                e.txn_id,
                e.table_name,
                e.op,
                dump_json(e.pk),
                dump_json(e.before),
                dump_json(e.after),
            ]
            for e in events
        ],
    )


def run(
    conn,
    ctx: Ctx,
    events: list[Event] | None,
    batch_id: str,
    now: datetime,
    overrides: frozenset[int] = frozenset(),
    persist: bool = True,
) -> dict:
    """Evaluate every rule for ``events`` (``None`` = full scan of the whole warehouse).

    Returns ``{"results": [...], "blocking": [...], "quarantined": [...]}``. With
    ``persist`` the per-rule results and quarantined rows are written (inside the caller's
    transaction). Violations on sequence numbers in ``overrides`` are downgraded to
    quarantine (operator override, OPERATIONS.md §4.2).
    """
    _stage_batch(conn, events)
    results, blocking, quarantined = [], [], []
    for rule_id, rule in RULES.items():
        found = []
        for table_name, sql in rule.build(ctx):
            for seq, pk, reason in conn.execute(sql).fetchall():
                found.append(
                    {
                        "rule_id": rule_id,
                        "table_name": table_name,
                        "seq": seq,
                        "pk": pk,
                        "reason": reason,
                        "severity": rule.severity,
                    }
                )
        for item in found:
            if item["severity"] == BLOCK and item["seq"] not in overrides:
                blocking.append(item)
            else:
                item["severity"] = QUARANTINE
                quarantined.append(item)
        results.append(
            {
                "rule_id": rule_id,
                "severity": rule.severity,
                "status": "fail" if found else "pass",
                "violations": len(found),
            }
        )
    if events is not None:
        batch_seqs = {e.seq for e in events}
        flagged = {q["seq"] for q in quarantined}
        for seq in sorted(overrides & batch_seqs - flagged):
            event = next(e for e in events if e.seq == seq)
            quarantined.append(
                {
                    "rule_id": "OVERRIDE",
                    "table_name": event.table_name,
                    "seq": seq,
                    "pk": dump_json(event.pk),
                    "reason": "quarantined by operator override",
                    "severity": QUARANTINE,
                }
            )
    if persist:
        stamp = naive(now)
        for r in results:
            conn.execute(
                "INSERT INTO quality.check_results VALUES (?, ?, ?, ?, ?, ?)",
                [
                    batch_id,
                    r["rule_id"],
                    r["severity"],
                    r["status"],
                    r["violations"],
                    stamp,
                ],
            )
        for q in quarantined:
            exists = conn.execute(
                "SELECT 1 FROM quality.quarantine WHERE seq = ? AND rule_id = ? AND pk = ?",
                [q["seq"], q["rule_id"], q["pk"]],
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO quality.quarantine (seq, table_name, pk, rule_id, reason, "
                    "batch_id, quarantined_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        q["seq"],
                        q["table_name"],
                        q["pk"],
                        q["rule_id"],
                        q["reason"],
                        batch_id,
                        stamp,
                    ],
                )
    return {"results": results, "blocking": blocking, "quarantined": quarantined}
