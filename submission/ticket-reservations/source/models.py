"""
Source schema for a ticketing and reservations system.

Strong entities: venues, events, customers, reservations.
Weak entities: seats, reservation_items, payments, tickets.

The local tests use DuckDB as a lightweight relational stand-in. Production DDL
for PostgreSQL is in sql/source/postgres_schema.sql.
"""

from __future__ import annotations

import duckdb


SCHEMA_CONTRACT: dict[str, dict[str, object]] = {
    "venues": {
        "pk": ["venue_id"],
        "columns": {
            "venue_id": "VARCHAR",
            "name": "VARCHAR",
            "city": "VARCHAR",
            "timezone": "VARCHAR",
            "created_at": "TIMESTAMP",
        },
    },
    "events": {
        "pk": ["event_id"],
        "columns": {
            "event_id": "VARCHAR",
            "venue_id": "VARCHAR",
            "name": "VARCHAR",
            "starts_at": "TIMESTAMP",
            "ends_at": "TIMESTAMP",
            "status": "VARCHAR",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "enum_domains": {"status": ["draft", "scheduled", "onsale", "closed", "cancelled"]},
    },
    "customers": {
        "pk": ["customer_id"],
        "columns": {
            "customer_id": "VARCHAR",
            "email": "VARCHAR",
            "full_name": "VARCHAR",
            "phone": "VARCHAR",
            "created_at": "TIMESTAMP",
        },
    },
    "seats": {
        "pk": ["seat_id"],
        "columns": {
            "seat_id": "VARCHAR",
            "venue_id": "VARCHAR",
            "section": "VARCHAR",
            "row_label": "VARCHAR",
            "seat_number": "VARCHAR",
            "status": "VARCHAR",
            "updated_at": "TIMESTAMP",
        },
        "enum_domains": {"status": ["available", "held", "booked", "blocked"]},
    },
    "reservations": {
        "pk": ["reservation_id"],
        "columns": {
            "reservation_id": "VARCHAR",
            "customer_id": "VARCHAR",
            "event_id": "VARCHAR",
            "status": "VARCHAR",
            "hold_expires_at": "TIMESTAMP",
            "confirmed_at": "TIMESTAMP",
            "total_amount": "DECIMAL(12,2)",
            "currency": "VARCHAR",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "enum_domains": {"status": ["held", "confirmed", "expired", "cancelled"]},
    },
    "reservation_items": {
        "pk": ["reservation_item_id"],
        "columns": {
            "reservation_item_id": "VARCHAR",
            "reservation_id": "VARCHAR",
            "seat_id": "VARCHAR",
            "unit_price": "DECIMAL(12,2)",
            "service_fee": "DECIMAL(12,2)",
            "currency": "VARCHAR",
            "created_at": "TIMESTAMP",
        },
    },
    "payments": {
        "pk": ["payment_id"],
        "columns": {
            "payment_id": "VARCHAR",
            "reservation_id": "VARCHAR",
            "provider": "VARCHAR",
            "provider_reference": "VARCHAR",
            "status": "VARCHAR",
            "amount": "DECIMAL(12,2)",
            "currency": "VARCHAR",
            "authorized_at": "TIMESTAMP",
            "captured_at": "TIMESTAMP",
            "refunded_at": "TIMESTAMP",
            "created_at": "TIMESTAMP",
        },
        "enum_domains": {"status": ["authorized", "captured", "failed", "refunded"]},
    },
    "tickets": {
        "pk": ["ticket_id"],
        "columns": {
            "ticket_id": "VARCHAR",
            "reservation_item_id": "VARCHAR",
            "event_id": "VARCHAR",
            "customer_id": "VARCHAR",
            "ticket_code": "VARCHAR",
            "status": "VARCHAR",
            "issued_at": "TIMESTAMP",
            "used_at": "TIMESTAMP",
        },
        "enum_domains": {"status": ["issued", "voided", "used"]},
    },
}


def contract_columns(table: str) -> dict[str, str]:
    return SCHEMA_CONTRACT[table]["columns"]  # type: ignore[return-value]


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS venues (
            venue_id   VARCHAR PRIMARY KEY,
            name       VARCHAR NOT NULL,
            city       VARCHAR NOT NULL,
            timezone   VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id   VARCHAR PRIMARY KEY,
            venue_id   VARCHAR NOT NULL REFERENCES venues(venue_id),
            name       VARCHAR NOT NULL,
            starts_at  TIMESTAMP NOT NULL,
            ends_at    TIMESTAMP NOT NULL,
            status     VARCHAR NOT NULL
                       CHECK (status IN ('draft', 'scheduled', 'onsale', 'closed', 'cancelled')),
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            CHECK (ends_at > starts_at)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR PRIMARY KEY,
            email       VARCHAR NOT NULL UNIQUE,
            full_name   VARCHAR NOT NULL,
            phone       VARCHAR,
            created_at  TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS seats (
            seat_id     VARCHAR PRIMARY KEY,
            venue_id    VARCHAR NOT NULL REFERENCES venues(venue_id),
            section     VARCHAR NOT NULL,
            row_label   VARCHAR NOT NULL,
            seat_number VARCHAR NOT NULL,
            status      VARCHAR NOT NULL
                        CHECK (status IN ('available', 'held', 'booked', 'blocked')),
            updated_at  TIMESTAMP NOT NULL,
            UNIQUE (venue_id, section, row_label, seat_number)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reservations (
            reservation_id VARCHAR PRIMARY KEY,
            customer_id    VARCHAR NOT NULL REFERENCES customers(customer_id),
            event_id       VARCHAR NOT NULL REFERENCES events(event_id),
            status         VARCHAR NOT NULL
                           CHECK (status IN ('held', 'confirmed', 'expired', 'cancelled')),
            hold_expires_at TIMESTAMP,
            confirmed_at   TIMESTAMP,
            total_amount   DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0),
            currency       VARCHAR NOT NULL,
            created_at     TIMESTAMP NOT NULL,
            updated_at     TIMESTAMP NOT NULL,
            CHECK ((status = 'held' AND hold_expires_at IS NOT NULL)
                   OR status IN ('confirmed', 'expired', 'cancelled'))
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reservation_items (
            reservation_item_id VARCHAR PRIMARY KEY,
            reservation_id      VARCHAR NOT NULL REFERENCES reservations(reservation_id),
            seat_id             VARCHAR NOT NULL REFERENCES seats(seat_id),
            unit_price          DECIMAL(12, 2) NOT NULL CHECK (unit_price >= 0),
            service_fee         DECIMAL(12, 2) NOT NULL DEFAULT 0 CHECK (service_fee >= 0),
            currency            VARCHAR NOT NULL,
            created_at          TIMESTAMP NOT NULL,
            UNIQUE (reservation_id, seat_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id         VARCHAR PRIMARY KEY,
            reservation_id     VARCHAR NOT NULL REFERENCES reservations(reservation_id),
            provider           VARCHAR NOT NULL,
            provider_reference VARCHAR NOT NULL,
            status             VARCHAR NOT NULL
                               CHECK (status IN ('authorized', 'captured', 'failed', 'refunded')),
            amount             DECIMAL(12, 2) NOT NULL CHECK (amount >= 0),
            currency           VARCHAR NOT NULL,
            authorized_at      TIMESTAMP,
            captured_at        TIMESTAMP,
            refunded_at        TIMESTAMP,
            created_at         TIMESTAMP NOT NULL,
            UNIQUE (provider, provider_reference),
            CHECK (captured_at IS NULL OR authorized_at IS NULL OR captured_at >= authorized_at)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id           VARCHAR PRIMARY KEY,
            reservation_item_id VARCHAR NOT NULL UNIQUE REFERENCES reservation_items(reservation_item_id),
            event_id            VARCHAR NOT NULL REFERENCES events(event_id),
            customer_id         VARCHAR NOT NULL REFERENCES customers(customer_id),
            ticket_code         VARCHAR NOT NULL UNIQUE,
            status              VARCHAR NOT NULL CHECK (status IN ('issued', 'voided', 'used')),
            issued_at           TIMESTAMP NOT NULL,
            used_at             TIMESTAMP,
            CHECK (used_at IS NULL OR used_at >= issued_at)
        )
    """)
