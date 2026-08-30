"""Historical recovery: reconstruct a prior point in time, and restore from it.

There are two restore primitives and the difference between them matters:

* ``as_of(entity, timestamp)`` — the latest version committed at or before an
  instant. Convenient, and what an operator asks for.
* ``as_of_version(entity, version)`` — exact. Event ids are a total order, so
  landing between two specific changes is always unambiguous.

The source clock has millisecond resolution, so two changes to the same row can
share a timestamp. Tests that need to land *between* adjacent versions use the
version primitive; that is not a workaround, it is the reason the version
primitive exists. Delta Lake draws the same line between TIMESTAMP AS OF and
VERSION AS OF.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from cdclake import source as source_mod


def _stamp(moment) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _versions(platform, entity: str, key: str, value) -> list[dict]:
    return [r for r in platform.warehouse.history(entity) if r[key] == value]


def test_as_of_version_returns_the_state_before_an_update(reference_platform):
    """Order 1001 walks PENDING -> PAID -> SHIPPED -> DELIVERED."""
    history = _versions(reference_platform, "customer_order", "order_id", 1001)
    assert [r["status"] for r in history] == [
        "PENDING",
        "PAID",
        "SHIPPED",
        "DELIVERED",
    ]

    for index, expected_status in enumerate(r["status"] for r in history):
        at_version = history[index]["_version"]
        rows = reference_platform.warehouse.as_of_version("customer_order", at_version)
        order = next(r for r in rows if r["order_id"] == 1001)
        assert order["status"] == expected_status


def test_as_of_before_creation_excludes_the_entity(reference_platform):
    history = _versions(reference_platform, "customer_order", "order_id", 1001)
    before_everything = _stamp(history[0]["valid_from"] - timedelta(seconds=1))

    rows = reference_platform.warehouse.as_of("customer_order", before_everything)
    assert not [r for r in rows if r["order_id"] == 1001]

    rows = reference_platform.warehouse.as_of_version(
        "customer_order", history[0]["_version"] - 1
    )
    assert not [r for r in rows if r["order_id"] == 1001]


def test_as_of_the_first_version_returns_the_original_state(reference_platform):
    history = _versions(reference_platform, "customer_order", "order_id", 1001)
    rows = reference_platform.warehouse.as_of_version(
        "customer_order", history[0]["_version"]
    )
    order = next(r for r in rows if r["order_id"] == 1001)

    assert order["status"] == "PENDING"
    assert order["total_amount"] == Decimal("177.00")


def test_a_deleted_entity_can_be_recovered_from_before_the_delete(reference_platform):
    """The requirement in one test: move backwards past a hard delete."""
    history = _versions(reference_platform, "customer_order", "order_id", 1004)
    tombstone = history[-1]
    assert tombstone["is_deleted"] is True

    recovered = reference_platform.warehouse.as_of_version(
        "customer_order", tombstone["_version"] - 1
    )
    order = next(r for r in recovered if r["order_id"] == 1004)

    assert order["status"] == "CANCELLED"
    assert order["total_amount"] == Decimal("96.00")

    # And at the delete itself it is gone again.
    after = reference_platform.warehouse.as_of_version(
        "customer_order", tombstone["_version"]
    )
    assert not [r for r in after if r["order_id"] == 1004]


def test_child_rows_are_recoverable_alongside_their_parent(reference_platform):
    """A restore is only useful if the weak entities come back too."""
    tombstones = [
        r
        for r in reference_platform.warehouse.history("order_item")
        if r["order_id"] == 1004 and r["is_deleted"]
    ]
    assert tombstones

    lines = [
        r
        for r in reference_platform.warehouse.as_of_version(
            "order_item", tombstones[0]["_version"] - 1
        )
        if r["order_id"] == 1004
    ]
    assert lines
    assert lines[0]["extended_amount"] == Decimal("96.00")


def test_as_of_now_equals_current_state(reference_platform):
    """The two views must never disagree — the boundary case people skip."""
    latest = max(
        r["valid_from"] for r in reference_platform.warehouse.history("customer_order")
    )
    as_of_now = {
        r["order_id"]: r["status"]
        for r in reference_platform.warehouse.as_of(
            "customer_order", _stamp(latest + timedelta(seconds=1))
        )
    }
    current = {
        r["order_id"]: r["status"]
        for r in reference_platform.warehouse.current("customer_order")
    }
    assert as_of_now == current


def test_timestamp_restore_resolves_collisions_to_the_latest_version(
    reference_platform,
):
    """When two versions share a commit timestamp the answer must still be
    deterministic: the later version wins, never an arbitrary one."""
    history = _versions(reference_platform, "customer_order", "order_id", 1001)
    for version in history:
        rows = reference_platform.warehouse.as_of(
            "customer_order", _stamp(version["valid_from"])
        )
        matches = [r for r in rows if r["order_id"] == 1001]
        assert len(matches) <= 1

        same_instant = [v for v in history if v["valid_from"] == version["valid_from"]]
        expected = max(same_instant, key=lambda v: v["_version"])
        if not expected["is_deleted"]:
            assert matches[0]["_version"] == expected["_version"]


def test_restore_output_can_be_written_back_to_the_source(
    seeded_platform, workspace_root
):
    """Restore is not a report; it has to produce rows the source accepts.

    The deleted order is read back out of history and re-inserted into a source
    database that still enforces every original constraint.
    """
    seeded_platform.ingest()
    source_mod.run_workload(seeded_platform.conn)
    seeded_platform.ingest()
    seeded_platform.refresh_warehouse()

    tombstone = [
        r
        for r in seeded_platform.warehouse.history("customer_order")
        if r["order_id"] == 1004 and r["is_deleted"]
    ][0]
    order = next(
        r
        for r in seeded_platform.warehouse.as_of_version(
            "customer_order", tombstone["_version"] - 1
        )
        if r["order_id"] == 1004
    )

    source_mod.insert(
        seeded_platform.conn,
        "customer_order",
        order_id=order["order_id"],
        customer_id=order["customer_id"],
        status=order["status"],
        currency=order["currency"],
        total_amount=float(order["total_amount"]),
        placed_at=order["placed_at"],
        cancelled_at=order["cancelled_at"],
        created_at=order["created_at"],
        updated_at=order["updated_at"],
    )

    restored = seeded_platform.conn.execute(
        "SELECT status, total_amount FROM customer_order WHERE order_id = 1004"
    ).fetchone()
    assert restored["status"] == "CANCELLED"
    assert restored["total_amount"] == 96.0

    # The restore is itself a change: it must flow back through CDC and land in
    # the warehouse like any other insert.
    seeded_platform.ingest()
    seeded_platform.refresh_warehouse()
    assert 1004 in {
        r["order_id"] for r in seeded_platform.warehouse.current("customer_order")
    }
