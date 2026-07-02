-- PostgreSQL source schema for the ticketing reservation system.

CREATE TYPE event_status AS ENUM ('draft', 'scheduled', 'onsale', 'closed', 'cancelled');
CREATE TYPE seat_status AS ENUM ('available', 'held', 'booked', 'blocked');
CREATE TYPE reservation_status AS ENUM ('held', 'confirmed', 'expired', 'cancelled');
CREATE TYPE payment_status AS ENUM ('authorized', 'captured', 'failed', 'refunded');
CREATE TYPE ticket_status AS ENUM ('issued', 'voided', 'used');

CREATE TABLE venues (
  venue_id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  timezone TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
  event_id UUID PRIMARY KEY,
  venue_id UUID NOT NULL REFERENCES venues(venue_id),
  name TEXT NOT NULL,
  starts_at TIMESTAMPTZ NOT NULL,
  ends_at TIMESTAMPTZ NOT NULL,
  status event_status NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (ends_at > starts_at)
);

CREATE TABLE customers (
  customer_id UUID PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  full_name TEXT NOT NULL,
  phone TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE seats (
  seat_id UUID PRIMARY KEY,
  venue_id UUID NOT NULL REFERENCES venues(venue_id),
  section TEXT NOT NULL,
  row_label TEXT NOT NULL,
  seat_number TEXT NOT NULL,
  status seat_status NOT NULL DEFAULT 'available',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (venue_id, section, row_label, seat_number)
);

CREATE TABLE reservations (
  reservation_id UUID PRIMARY KEY,
  customer_id UUID NOT NULL REFERENCES customers(customer_id),
  event_id UUID NOT NULL REFERENCES events(event_id),
  status reservation_status NOT NULL,
  hold_expires_at TIMESTAMPTZ,
  confirmed_at TIMESTAMPTZ,
  total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  currency CHAR(3) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((status = 'held' AND hold_expires_at IS NOT NULL)
         OR status IN ('confirmed', 'expired', 'cancelled'))
);

CREATE TABLE reservation_items (
  reservation_item_id UUID PRIMARY KEY,
  reservation_id UUID NOT NULL REFERENCES reservations(reservation_id),
  seat_id UUID NOT NULL REFERENCES seats(seat_id),
  unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  service_fee NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (service_fee >= 0),
  currency CHAR(3) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (reservation_id, seat_id)
);

CREATE TABLE payments (
  payment_id UUID PRIMARY KEY,
  reservation_id UUID NOT NULL REFERENCES reservations(reservation_id),
  provider TEXT NOT NULL,
  provider_reference TEXT NOT NULL,
  status payment_status NOT NULL,
  amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  currency CHAR(3) NOT NULL,
  authorized_at TIMESTAMPTZ,
  captured_at TIMESTAMPTZ,
  refunded_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (provider, provider_reference),
  CHECK (captured_at IS NULL OR authorized_at IS NULL OR captured_at >= authorized_at)
);

CREATE TABLE tickets (
  ticket_id UUID PRIMARY KEY,
  reservation_item_id UUID NOT NULL UNIQUE REFERENCES reservation_items(reservation_item_id),
  event_id UUID NOT NULL REFERENCES events(event_id),
  customer_id UUID NOT NULL REFERENCES customers(customer_id),
  ticket_code TEXT NOT NULL UNIQUE,
  status ticket_status NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  used_at TIMESTAMPTZ,
  CHECK (used_at IS NULL OR used_at >= issued_at)
);

CREATE INDEX idx_events_venue_start ON events (venue_id, starts_at);
CREATE INDEX idx_seats_venue_status ON seats (venue_id, status);
CREATE INDEX idx_reservations_customer_created ON reservations (customer_id, created_at DESC);
CREATE INDEX idx_reservations_event_status ON reservations (event_id, status);
CREATE INDEX idx_reservation_items_reservation ON reservation_items (reservation_id);
CREATE INDEX idx_payments_reservation_status ON payments (reservation_id, status);
CREATE INDEX idx_tickets_event_status ON tickets (event_id, status);

