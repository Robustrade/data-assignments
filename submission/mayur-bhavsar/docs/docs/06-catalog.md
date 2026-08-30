# Catalog and access

`catalog/datasets.yml` publishes 15 datasets — 1 lake, 14 warehouse — and
`scripts/validate_catalog.py` checks the publication against reality on every
CI run. A catalog nobody verifies is documentation, not metadata.

---

## What is published

Every entry carries, and is required to carry:

| Field | Why a consumer needs it |
|---|---|
| `name` | Prefixed with its layer (`lake.` / `warehouse.`) so change history can never be mistaken for current state |
| `layer` | Which of the two it is |
| `owner` | Who to ask, resolved against an `owners:` block with contact and on-call channel |
| `description` | What it is |
| `intended_consumers` | Who it is for |
| `not_intended_for` | On `lake.cdc_events`: the misuse that would actually happen |
| `update_cadence` | How fresh it is |
| `storage`, `location`, `access` | How to query it |
| `contains_pii` (+ `pii_notes`) | Whether it is subject to erasure obligations |
| `lineage` | What it is derived from, by catalog name |
| schema | Column list, either explicit or `columns_from_contract` + `column_group` |

`columns_from_contract` avoids restating the entity columns that
`contracts/<table>.json` already publishes; the validator expands the reference
before comparing. So the catalog cannot describe a different schema from the
one the contract publishes.

---

## The two layers, and the line between them

The rubric's warning about "muddled" lake and warehouse access is the reason
`lake.cdc_events` carries a `not_intended_for` field:

> Analytics. It is a change log, not a snapshot; querying it directly for
> current state will double-count updated rows.

| | `lake.cdc_events` | `warehouse.*_current` | `warehouse.dim_*_history` | `warehouse.mart_*` |
|---|---|---|---|---|
| For | The warehouse build; platform engineers doing replay or forensics | Analysts, BI, downstream services | Restore, audit, slowly-changing analysis | Finance and analysts |
| Grain | One row per change | One row per live entity | One row per version | One row per order |
| Owner | `data-platform` | `commerce-analytics` | `data-platform` | `commerce-analytics` |
| Cadence | Continuous micro-batch | Every warehouse build | Every warehouse build | Every warehouse build |

Ownership splits along that line deliberately: the platform team owns the
change log and the history models, because those are correctness surfaces; the
analytics team owns the current-state views and the marts, because those are
product surfaces.

---

## PII

Flagged per dataset. The one worth pointing at is `lake.cdc_events`, which is
`contains_pii: true` with a note:

> before/after images carry customer email, name and phone. Retention and
> erasure obligations apply to the lake, not only to the warehouse.

This is the most commonly missed consequence of "the lake keeps every change".
An erasure request cannot be satisfied by deleting a warehouse row: the lake
holds every historical value of that row, and the design otherwise forbids
rewriting lake files. See [docs/04](04-time-travel.md#retention).

---

## How drift is caught

`scripts/validate_catalog.py` runs four checks, the last against a warehouse
built from scratch during the run:

1. **Required metadata is present on every dataset** — including that `owner`
   resolves to a real entry in `owners:`, and that the name is layer-prefixed.
2. **The catalogued lake schema equals the physical lake record** — compared
   against `cdclake.lake.LAKE_COLUMNS`, the actual field order written to disk.
3. **Catalog and dbt models agree in both directions.** Both directions is the
   point: the forward check stops the catalog advertising a dataset that does
   not exist; the reverse check stops a model shipping undocumented. The second
   is the one teams skip.
4. **Catalogued schemas equal the live relations** — read from
   `information_schema.columns` after a real build, so a column added to a model
   without a catalog update fails CI.

Both failure directions are themselves tested
([`tests/test_catalog.py`](../tests/test_catalog.py)): removing a dataset from
the catalog must be detected, and removing a required field must be detected.

---

## How a consumer finds and queries a dataset

```bash
# What exists, who owns it, what it is for
cat catalog/datasets.yml

# Current state — the default starting point
duckdb .work/demo/warehouse/warehouse.duckdb \
  -c "select * from customer_order_current limit 10"

# Cross-entity analytics
duckdb .work/demo/warehouse/warehouse.duckdb \
  -c "select * from mart_order_financials order by outstanding_amount desc"

# History, for audit or restore
duckdb .work/demo/warehouse/warehouse.duckdb \
  -c "select * from dim_customer_order_history where order_id = 1001 order by _version"

# The change log itself — platform use only
duckdb -c "select * from read_json('.work/demo/lake/cdc_events/**/*.ndjson',
           format='newline_delimited') order by event_id limit 20"
```

`dbt docs generate && dbt docs serve` renders the model-level descriptions,
column documentation and lineage graph from `models/schema.yml` and
`models/sources.yml`, which is the same metadata in a browsable form.

---

## Production analogue

| Here | Production |
|---|---|
| `catalog/datasets.yml` | Unity Catalog, AWS Glue Data Catalog, DataHub or OpenMetadata |
| `validate_catalog.py` in CI | The same check, run against the catalog API on deploy |
| `owner` + `on_call` fields | Ownership metadata synced from the team directory |
| `contains_pii` flag | A governance tag that drives masking policies and access grants |
| `lineage` list | Column-level lineage harvested from the dbt manifest |
| File paths in `location` | Catalog-registered external tables and warehouse schemas |

The important part is not the file format. It is that the metadata is
**derived, required and verified** rather than hand-maintained prose — which is
what makes it survive contact with a changing pipeline.
