# Source Data Model

Domain: an order-management slice of e-commerce. Customers place orders;
orders contain line items and are paid via one or more payment attempts.

Defined in `source/models.py` (`EXPECTED_COLUMNS`, `PRIMARY_KEYS`,
`COLUMN_TYPES`, `init_source_schema`).

## Entities

| Table | Kind | Why |
|---|---|---|
| `customers` | Strong | Independent identity/lifecycle, not owned by anything else. |
| `orders` | Strong | Independent identity/lifecycle; references a customer but exists as its own entity with its own status lifecycle. |
| `order_items` | Weak | Lifecycle is entirely tied to its parent order — an order line item has no meaning outside an order. |
| `payments` | Weak | Tied to an order, but more loosely than `order_items` — a payment can arrive or retry after the order was created, so it carries its own timestamps and status independent of the order's. |

## Schema

```
customers
  customer_id   VARCHAR   PRIMARY KEY
  name          VARCHAR   NOT NULL
  email         VARCHAR   NOT NULL
  status        VARCHAR   NOT NULL CHECK (status IN ('active','suspended','closed'))
  created_at    TIMESTAMP NOT NULL
  updated_at    TIMESTAMP NOT NULL

orders
  order_id       VARCHAR   PRIMARY KEY
  customer_id    VARCHAR   NOT NULL REFERENCES customers(customer_id)
  status         VARCHAR   NOT NULL CHECK (status IN ('pending','paid','shipped','cancelled','refunded'))
  total_amount   DECIMAL(18,2) NOT NULL CHECK (total_amount >= 0)
  discount_code  VARCHAR   NULL
  created_at     TIMESTAMP NOT NULL
  updated_at     TIMESTAMP NOT NULL
  INDEX idx_orders_customer_id (customer_id)
  INDEX idx_orders_status (status)

order_items
  order_item_id  VARCHAR   PRIMARY KEY
  order_id       VARCHAR   NOT NULL   -- see FK note below
  sku            VARCHAR   NOT NULL
  quantity       INTEGER   NOT NULL CHECK (quantity > 0)
  unit_price     DECIMAL(18,2) NOT NULL CHECK (unit_price >= 0)
  INDEX idx_order_items_order_id (order_id)

payments
  payment_id   VARCHAR   PRIMARY KEY
  order_id     VARCHAR   NOT NULL   -- see FK note below
  amount       DECIMAL(18,2) NOT NULL CHECK (amount > 0)
  status       VARCHAR   NOT NULL CHECK (status IN ('pending','settled','failed','refunded'))
  created_at   TIMESTAMP NOT NULL
  paid_at      TIMESTAMP NULL
  INDEX idx_payments_order_id (order_id)
```

## Why `order_items.order_id` / `payments.order_id` have no `REFERENCES` clause

This is a deliberate workaround for a real DuckDB engine limitation, not a
modeling gap. DuckDB 1.5.5 cannot `UPDATE` a table that is simultaneously an
FK child (`orders` references `customers`) and an FK parent (`order_items`
and `payments` reference `orders`) — any `UPDATE` on `orders` fails with
"Violates foreign key constraint because key is still referenced," even
when the update doesn't touch `order_id` at all.

This was confirmed with an isolated minimal repro (a grandparent -> parent
-> child FK chain) before concluding it was an engine bug rather than a
mistake in this schema. Since `orders.status` changes constantly (the most
common write in this domain), `orders` has to stay updatable, so the FK
declaration was dropped on the child side. Referential integrity for this
link is enforced instead in the data-quality layer
(`check_referential_integrity` in `scripts/run_data_quality_checks.py`) —
same as every other business rule that can't be expressed as a
single-table constraint.

On Postgres or MySQL, this workaround would not be necessary — the FK
would be declared normally.

## Invariants

- `orders.total_amount` should reconcile with
  `sum(order_items.quantity * order_items.unit_price)` for that order —
  checked downstream (`check_order_totals_match_items`), not enforceable
  as a single-table `CHECK`.
- `order_items.quantity > 0`, `unit_price >= 0`.
- `payments.amount > 0`.
- `payments.paid_at >= payments.created_at` when `paid_at` is set —
  checked downstream (`check_payment_timing`).
- Status fields are restricted to known enum values at the source, and
  re-verified downstream (`check_enum_domain`) since the warehouse doesn't
  physically enforce the same `CHECK` constraints.
- Status transitions follow a legal state machine per entity — checked
  downstream using the lake's full history (`check_status_transitions`),
  since this needs the sequence of past statuses, not just the current one:
  - orders: `pending -> {paid, cancelled}`, `paid -> {shipped, refunded}`,
    `shipped -> {refunded}`; `cancelled`/`refunded` are terminal.
  - payments: `pending -> {settled, failed}`, `settled -> {refunded}`;
    `failed`/`refunded` are terminal.
- `order_items.order_id` / `payments.order_id` should reference a real
  order — not a DuckDB-enforced FK for the reason above, checked instead
  in the data-quality layer.

## Change Patterns

- `customers`: created once, status updated occasionally (active →
  suspended/closed); rarely hard-deleted in practice (soft-deletion via
  status is the realistic pattern, though the pipeline supports a real
  source-side delete too, captured as a soft-delete in the warehouse).
- `orders`: created once, status updated multiple times over its
  lifecycle (this is the highest-frequency write pattern in the domain).
- `order_items`: created with the order, essentially immutable afterward.
- `payments`: created when a payment attempt starts, updated as it
  settles/fails; a single order can have more than one payment row
  (retries).
