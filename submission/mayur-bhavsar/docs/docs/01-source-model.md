# Source model

Domain: **order-to-cash for a small e-commerce business**. Chosen because its
invariants are real rather than decorative — a line-item total that must equal
an order header, a status that may only walk certain paths, a settlement that
cannot precede its own creation — and because it has a natural mix of strong
and weak entities and a genuinely high-churn table.

DDL: [`sql/001_source_schema.sql`](../sql/001_source_schema.sql).
Capture: [`sql/002_cdc_triggers.sql`](../sql/002_cdc_triggers.sql).

---

## Entities

```
customer ──1:N──▶ customer_order ──1:N──▶ order_item ◀──N:1── product
                        │
                        └──1:N──▶ payment ──1:N──▶ payment_attempt
```

| Table | Kind | Why it exists | Change pattern |
|---|---|---|---|
| `customer` | strong | Exists independently and is referenced by orders. | Low volume: profile edits, status transitions. |
| `product` | strong | Sellable item, independent lifecycle. | Slowly changing. Price edits are the interesting case: they must not retroactively alter historical order lines. |
| `customer_order` | strong | The order header, and the highest-value row in the domain. | High churn: `PENDING → PAID → SHIPPED → DELIVERED`, or `CANCELLED`. |
| `order_item` | **weak** | A line within an order. | Written once with the order; occasionally amended before payment. |
| `payment` | strong (dependent) | Money against an order. Has its own surrogate key and its own lifecycle, so it is not weak — but it cannot exist without an order. | Moderate: `INITIATED → SETTLED / FAILED / REFUNDED`. |
| `payment_attempt` | **weak** | One gateway call. | Highest churn in the domain; retries are what make CDC volume realistic. |

### What makes an entity weak here

Two things together, not one:

1. **Identity depends on the parent.** `order_item.line_no` means nothing
   without `order_id`; `payment_attempt.attempt_no` means nothing without
   `payment_id`. Both have composite primary keys, `(order_id, line_no)` and
   `(payment_id, attempt_no)`.
2. **Existence depends on the parent.** Both use `ON DELETE CASCADE`.

`payment` fails the first test — `payment_id` is a surrogate key that is unique
on its own — so it is modelled as a strong entity with a mandatory parent, and
it deliberately does **not** cascade. Deleting an order should never silently
destroy financial records; the foreign key forces an erasure to walk the graph
explicitly. That asymmetry is exercised by
`workload_deletes` and asserted in `test_cascaded_child_deletes_are_captured`.

The consequence downstream is that weak entities need a key that behaves like a
surrogate one. That is what `pk_hash` — a stable hash of the canonical primary
key JSON — is for: composite keys and single-column keys merge identically.

---

## Data types

| Concern | Modelling | Note |
|---|---|---|
| Money | `DECIMAL(12,2)`, `CHECK (x >= 0 AND ROUND(x, 2) = x)` | SQLite applies NUMERIC affinity, so this is physically a float. The `ROUND` guard keeps values at two decimal places and the warehouse casts once to `DECIMAL(18,2)`, making downstream arithmetic exact. Postgres `NUMERIC(12,2)` would be exact at the source too. |
| Timestamps | `TEXT`, strict UTC ISO-8601 with milliseconds | Lexicographically sortable, and byte-identical to SQLite's `strftime('%Y-%m-%dT%H:%M:%fZ')` so trigger-written and application-written values compare correctly. |
| Enums | `TEXT` + `CHECK ... IN (...)` | The gate parses the domain out of `sqlite_master` and publishes it in the contract. Postgres would use a native `ENUM` type; the contract shape is the same either way. |
| Identifiers | `INTEGER PRIMARY KEY`, composite keys on weak entities | Note SQLite reports `notnull = 0` for an `INTEGER PRIMARY KEY` even though it can never be NULL. `introspect_table` corrects this, otherwise every surrogate key would lose its not-null assertion downstream. |
| Nullable attributes | `customer.phone`, `product.discontinued_at`, `customer_order.cancelled_at`, `payment.settled_at`, `payment_attempt.failure_code` | Each is genuinely optional, and each has a conditional rule attached (see below) so nullability is not just absence. |

---

## Invariants

### Enforced by the database

| Rule | Where |
|---|---|
| Primary keys, including composite keys on weak entities | `PRIMARY KEY` |
| `customer.email` and `product.sku` unique | `UNIQUE` |
| Referential integrity, with `ON DELETE CASCADE` only on weak entities | `FOREIGN KEY` + `PRAGMA foreign_keys = ON` |
| Status, method and result domains | `CHECK ... IN (...)` |
| No negative money anywhere; two-decimal precision | `CHECK` |
| `extended_amount = quantity * unit_price` | `CHECK` (single-row, so expressible) |
| `cancelled_at` only on a `CANCELLED` order, never before `placed_at` | two `CHECK`s |
| `settled_at` never before `created_at`, and only on `SETTLED`/`REFUNDED` | two `CHECK`s |
| A `SUCCESS` attempt has no failure code; a non-success has one | `CHECK` |
| The change log's own shape: `I` has no before image, `D` has no after image | `CHECK` on `cdc_events` |

Every one of these is asserted by trying to violate it, in
[`tests/test_source_constraints.py`](../tests/test_source_constraints.py).

### Not enforceable by the database

| Rule | Why not | Where it is checked |
|---|---|---|
| `sum(order_item.extended_amount) = customer_order.total_amount` | Spans rows | Application layer at the source; `dbt_tests/assert_order_total_matches_line_items.sql` downstream |
| Legal status transitions | Needs the previous version | Application layer; `dbt_tests/assert_legal_order_status_transitions.sql` — expressible downstream *only* because the warehouse keeps history |

This split is the whole reason validation parity matters. A rule with no
database constraint behind it is precisely the rule most likely to be violated,
and therefore the one the warehouse most needs to re-assert. See
[docs/05](05-validation-parity.md).

---

## Indexes

Each exists for a stated pattern, not for decoration.

| Index | Supports |
|---|---|
| `customer_order (customer_id, placed_at)` | Order history for a customer — the commonest transactional lookup |
| `customer_order (updated_at)` | The timestamp-watermark extraction fallback, and "what changed recently" |
| `order_item (product_id)` | Referential assertions and product-level revenue rollups |
| `payment (order_id)` | Payments are almost always fetched by their parent order |
| `payment (status, created_at)` | Settlement monitoring: unsettled payments older than N minutes |
| `payment_attempt (result, attempted_at)` | Retry analysis on the highest-churn table |
| `cdc_events (table_name, event_id)` | The consumer's forward scan from its checkpoint |

Indexes are part of the published contract. Losing one is classified
`COMPATIBLE` — correctness is unaffected — but it raises a warning, because
extraction latency will regress. `test_indexes_exist_for_the_documented_access_patterns`
pins them.

---

## Seed and workload

`cdclake/source.py` provides a deterministic starting state (4 customers,
5 products, 4 orders, 6 lines, 4 payments) and seven named workload phases that
between them produce 46 change events:

| Phase | Exercises |
|---|---|
| `settle_payments` | The happy path: attempt succeeds, payment settles, order becomes `PAID` |
| `retry_storm` | High churn on a weak entity: two failures then a success |
| `price_change` | A product price edit that must not rewrite history |
| `ship_and_deliver` | A multi-step status walk on one key |
| `cancellation` | A conditional-nullable rule (`cancelled_at`) firing |
| `soft_churn` | Two updates to the same key in quick succession — version ordering |
| `deletes` | An explicit delete, and a cascade that must reach the lake |
