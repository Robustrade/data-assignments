# Schema Safety and Data Quality

Covers: `scripts/check_schema_contracts.py`, `scripts/run_data_quality_checks.py`.

## Schema-contract check: fail-closed on breaking changes

`check_schema_contracts.py` compares the live source's
`information_schema.columns` against an expected-columns/types contract
defined in `source/models.py` (`EXPECTED_COLUMNS`, `COLUMN_TYPES`).

Two categories of difference, handled differently:

- **Breaking** — a dropped column, a renamed column (which looks like a
  drop), or a changed type. `enforce()` raises `SchemaContractViolation`;
  the top-level script catches it, prints every specific problem to
  stderr, and exits with a non-zero code. Ingestion should stop here
  rather than keep writing against a wrong assumption about the schema.
- **Non-breaking** — a genuinely new column that isn't in the contract
  yet. Reported as a warning, not a failure: old code simply won't
  reference a column it doesn't know about, so nothing crashes. It's a
  signal that the contract needs updating, not a reason to stop.

**Current gap**: nothing automated currently watches for that non-zero
exit code — it's a manually-run script. In production this would be
wired into an orchestrator (e.g. Airflow) so a schema-contract failure
fails the task and pages someone, or pauses the downstream pipeline.

## Data-quality checks: putting back the constraints the warehouse doesn't have

The warehouse tables are typed loosely on purpose (see
`warehouse_and_time_travel.md`) so the merge logic stays generic — which
means none of the source's constraints (`NOT NULL`, `CHECK`, `FK`)
physically exist on the warehouse side. `run_data_quality_checks.py` puts
that parity back: every rule the source enforces gets re-checked here
against the warehouse, so a CDC bug or a late/out-of-order write can't
silently corrupt it without anyone noticing.

### System checks

- `check_pk_uniqueness` — no duplicate primary keys per table.
- `check_not_null` — required columns aren't null.
- `check_referential_integrity` — every FK-shaped column resolves to a
  real parent row, including `orders -> customers` (which *is* a real DB
  FK) and `order_items/payments -> orders` (which is *not* a DB FK, for
  the DuckDB engine-limitation reason documented in `source.md` — this is
  the one place that link's integrity is actually enforced).
- `check_enum_domain` — status columns only contain known values.

### Business checks

- `check_non_negative_amounts` — order totals, item quantities/prices,
  payment amounts are all within their valid range.
- `check_order_totals_match_items` — `orders.total_amount` reconciles
  with `sum(order_items.quantity * unit_price)` for that order.
- `check_payment_timing` — `payments.paid_at >= payments.created_at`.
- `check_status_transitions` — the one check that needs more than the
  warehouse's latest state. It replays the lake's *full* history per
  entity (via `_status_history_per_entity`, sorted by `sequence`) and
  flags any transition that isn't in the allowed state machine (e.g.
  `cancelled -> paid` is illegal for an order). This structurally
  requires the lake, not the warehouse, because the warehouse has no
  memory of what a status used to be.

### Where the lake events come from

`_read_lake_events()` reads directly from the Parquet lake via
`read_parquet('data/lake_parquet/*/*.parquet')`, reusing the same DuckDB
connection as the warehouse check (any open DuckDB connection can query a
Parquet file by path — it doesn't have to be "the lake's own"
connection). `run_all()`/`enforce()` take this as a plain list of event
dicts, not a database connection, which is also what lets the test suite
build the same shape of list by hand without touching Parquet at all.

### Failure surfacing

Same pattern as the schema-contract check: `enforce()` raises
`DataQualityViolation` listing every failure by category, caught at the
top level, exits non-zero. Same current gap: nothing automated currently
watches for that exit code.
