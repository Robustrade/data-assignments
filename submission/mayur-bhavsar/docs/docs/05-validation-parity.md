# Validation parity

The requirement is that source validations also exist in the warehouse. The
hard part is not writing them twice — it is stopping the two copies from
drifting apart six months later.

So parity here is **checked mechanically**. `cdclake/quality.py:check_validation_parity`
reads `contracts/*.json` and `models/schema.yml` and pairs them up. If a source
constraint has no downstream twin, the build fails.

```
make quality      # 65 parity checks, among 87 platform checks
make test         # the same checker, plus proof that it can fail
```

---

## The traceability matrix

Generated from the contracts at check time; reproduced here in full.

### System validations

| Source rule | Declared in | Warehouse assertion | Checked by |
|---|---|---|---|
| Primary key uniqueness (all 6 tables) | `PRIMARY KEY` | `unique` + `not_null` on `<entity>_current.pk_hash`, and on the key column for single-column keys | `parity::*.pk_hash`, `parity::*.pk_unique` |
| `customer.email` unique | `UNIQUE` | `unique` on `customer_current.email` | `parity::customer_current.email.unique` |
| `product.sku` unique | `UNIQUE` | `unique` on `product_current.sku` | `parity::product_current.sku.unique` |
| NOT NULL — 46 columns across 6 tables | `NOT NULL` | `not_null` on the matching current-model column | `parity::*.not_null` |
| `customer_order.customer_id → customer` | `FOREIGN KEY` | `relationships` to `ref('customer_current')` | `parity::*.relationships` |
| `order_item.order_id → customer_order` | `FOREIGN KEY` | `relationships` | ” |
| `order_item.product_id → product` | `FOREIGN KEY` | `relationships` | ” |
| `payment.order_id → customer_order` | `FOREIGN KEY` | `relationships` | ” |
| `payment_attempt.payment_id → payment` | `FOREIGN KEY` | `relationships` | ” |
| `customer.status` domain | `CHECK ... IN` | `accepted_values`, exact set | `parity::*.accepted_values` |
| `customer_order.status` domain | `CHECK ... IN` | `accepted_values`, exact set | ” |
| `payment.method` domain | `CHECK ... IN` | `accepted_values`, exact set | ” |
| `payment.status` domain | `CHECK ... IN` | `accepted_values`, exact set | ” |
| `payment_attempt.result` domain | `CHECK ... IN` | `accepted_values`, exact set | ” |
| Change-log shape (`I` has no before image, etc.) | `CHECK` on `cdc_events` | `accepted_values` on `stg_cdc_events.op`; `unique` on `event_key` | `models/schema.yml` |

### Business validations

These are the ones the source *cannot* enforce, which is exactly why they
matter downstream.

| Rule | Enforced at the source by | Warehouse assertion |
|---|---|---|
| `sum(order_item.extended_amount) = order.total_amount` | Application only — spans rows | `assert_order_total_matches_line_items.sql` |
| Legal status transitions (`PENDING → PAID → SHIPPED → DELIVERED`; `CANCELLED` only from `PENDING`/`PAID`) | Application only — needs the previous version | `assert_legal_order_status_transitions.sql` |
| `extended_amount = quantity * unit_price` | `CHECK` | `assert_extended_amount_is_consistent.sql` |
| No negative money anywhere | `CHECK` × 5 | `assert_no_negative_amounts.sql` |
| `settled_at >= created_at` | `CHECK` | `assert_settlement_not_before_creation.sql` |
| `settled_at` only on `SETTLED`/`REFUNDED` | `CHECK` | `assert_settled_at_only_when_settled.sql` |
| `cancelled_at` only on `CANCELLED`, never before `placed_at` | `CHECK` × 2 | `assert_cancelled_orders_are_consistent.sql` |
| `SUCCESS` has no failure code; non-success has one | `CHECK` | `assert_successful_attempt_has_no_failure_code.sql` |

**Status-transition validation is only possible because the warehouse keeps
history.** A snapshot-only warehouse can check that a status is in its domain;
it cannot check that the row got there legally. Keeping every version turns a
transition into a row you can write SQL against.

### Platform invariants

Not mirrors of source rules — properties of the pipeline itself.

| Invariant | Checked by |
|---|---|
| One open SCD2 interval per key, in every history model | `assert_one_current_version_per_key.sql` |
| Intervals never end before they start; closed ≠ current | `assert_scd2_intervals_are_ordered.sql` |
| Deduplication drops no distinct change | `assert_lake_is_fully_represented.sql` |
| Every delete is a flagged history row, never a missing one | `assert_deletes_are_tombstoned_not_erased.sql` |
| Source and warehouse row counts agree, per table | `quality.check_reconciliation` |
| Every source change is in the lake, exactly once | `quality.check_lake_completeness` |
| Manifest files still hold the rows they claim | `quality.check_lake_integrity` |
| Batches chain without a gap | `quality.check_checkpoint_chain` |
| Worst observed lag within the 60s SLO | `quality.check_freshness` |
| Source foreign keys and page integrity | `quality.check_source_constraints` |

---

## Why the checker is the interesting part

Anyone can write a table like the one above. The question a reviewer should ask
is what happens when someone adds a `CHECK ... IN (...)` to the source next
quarter and forgets `models/schema.yml`.

Here, the answer is that CI goes red. The parity checker derives what *must*
exist from the published contract:

- every non-nullable column ⇒ a `not_null` assertion on `<entity>_current`;
- every enum domain ⇒ an `accepted_values` assertion whose value set is
  **exactly equal** to the contract's, not merely present;
- every single-column unique constraint and primary key ⇒ a `unique` assertion;
- every single-column foreign key ⇒ a `relationships` assertion pointing at
  `ref('<parent>_current')` on the right field.

And the checker is itself tested for its ability to fail.
[`tests/test_validation_parity.py`](../tests/test_validation_parity.py) mutates
a copy of `schema.yml` four ways — remove a `not_null`, narrow an enum domain,
remove a `relationships`, remove a `unique` — and asserts each is caught. A
parity check that only ever passes proves nothing.

---

## How failures surface

| Where | Signal |
|---|---|
| In-warehouse assertion fails | `dbt build` exits non-zero, naming the model, the test and the failing rows; CI job `data-quality-check` goes red |
| Parity check fails | `make quality` prints the specific missing assertion (`source declares email NOT NULL; customer_current has no not_null assertion`) and exits non-zero |
| Reconciliation drifts | Per-table source/warehouse/delta table in `make demo` and `make quality` |
| Freshness breaches the SLO | `freshness::lag` fails with the observed worst lag against the objective |
| Schema drift | The pipeline halts before ingesting; see [docs/03](03-schema-evolution.md) |

Nothing here is a warning that scrolls past. Every one of them fails a build.

---

## Gaps worth naming

- **The warehouse re-asserts rules; it does not quarantine rows.** A failing
  assertion fails the build, which stops bad data from being *published* but
  also stops good data from being refreshed. A production deployment would want
  the failing rows routed to a quarantine relation with the rest of the build
  proceeding, for the non-critical assertions at least.
- **Composite unique constraints and multi-column foreign keys are skipped by
  the parity checker** (there are none in this model). Adding one would silently
  go unchecked — which is why the checker should be extended at the same time
  as the first such constraint is introduced.
- **Freshness is measured source-to-lake, not source-to-warehouse.** The
  warehouse refresh is a separate step whose duration is logged but not folded
  into the SLO.
