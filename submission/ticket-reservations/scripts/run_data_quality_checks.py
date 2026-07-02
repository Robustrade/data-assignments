from __future__ import annotations

import duckdb


def create_quality_demo(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE reservations_current (
            reservation_id VARCHAR,
            status VARCHAR,
            total_amount DECIMAL(12, 2),
            is_deleted BOOLEAN
        )
    """)
    conn.execute("""
        CREATE TABLE reservation_items_current (
            reservation_id VARCHAR,
            unit_price DECIMAL(12, 2),
            service_fee DECIMAL(12, 2),
            is_deleted BOOLEAN
        )
    """)


def reservation_total_mismatches(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute("""
        SELECT r.reservation_id
        FROM reservations_current r
        JOIN reservation_items_current i
          ON r.reservation_id = i.reservation_id
        WHERE r.is_deleted = false
          AND i.is_deleted = false
        GROUP BY r.reservation_id, r.total_amount
        HAVING r.total_amount <> SUM(i.unit_price + i.service_fee)
    """).fetchall()


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_quality_demo(conn)
    failures = reservation_total_mismatches(conn)
    if failures:
        print(f"Reservation total mismatches: {failures}")
        return 1
    print("Data quality checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

