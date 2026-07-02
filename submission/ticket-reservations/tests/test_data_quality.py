from __future__ import annotations

from scripts.run_data_quality_checks import create_quality_demo, reservation_total_mismatches


def test_reservation_total_mismatch_is_detected():
    import duckdb

    conn = duckdb.connect(":memory:")
    create_quality_demo(conn)
    conn.execute("INSERT INTO reservations_current VALUES ('r1', 'held', 100.00, false)")
    conn.execute("INSERT INTO reservation_items_current VALUES ('r1', 80.00, 5.00, false)")

    assert reservation_total_mismatches(conn) == [("r1",)]


def test_matching_reservation_total_passes():
    import duckdb

    conn = duckdb.connect(":memory:")
    create_quality_demo(conn)
    conn.execute("INSERT INTO reservations_current VALUES ('r1', 'held', 85.00, false)")
    conn.execute("INSERT INTO reservation_items_current VALUES ('r1', 80.00, 5.00, false)")

    assert reservation_total_mismatches(conn) == []

