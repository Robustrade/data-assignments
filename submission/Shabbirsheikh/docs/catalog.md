# Catalog

Covers: `catalog/catalog.json`, `scripts/validate_catalog.py`.

## What's registered

`catalog.json` has 5 entries — the lake dataset and the four warehouse
tables. There is no source/raw-layer entry: the source database itself is
the transactional system of record, not a published, consumer-facing
dataset.

| Dataset | Layer |
|---|---|
| `lake_cdc_events` | lake |
| `wh_customers` | warehouse |
| `wh_orders` | warehouse |
| `wh_order_items` | warehouse |
| `wh_payments` | warehouse |

## What each entry documents

- `owner` — the team responsible for the dataset (`data-platform` for all
  of them here).
- `consumers` — which downstream teams are expected to use it, and this
  differs per table: `wh_payments` lists `finance` and `audit` as
  consumers, `wh_order_items` doesn't, reflecting who actually needs that
  data.
- `update_cadence` — how fresh the data is expected to be (e.g.
  "streaming — applied within one trigger interval of the source change
  in continuous mode, or on-demand").
- `description` — what the dataset is, in plain language.
- `schema` — every column, with type and a one-line meaning, including
  the two warehouse-only bookkeeping columns (`_cdc_seq`, `_deleted`).

`lake_cdc_events`'s entry specifically documents that it's Parquet,
day-partitioned under `data/lake_parquet/event_date=YYYY-MM-DD/`, and that
`sequence` is Kafka's own message offset — someone reading the catalog
shouldn't need to read the pipeline code to understand what they're
querying.

## Enforcement, not just documentation

`validate_catalog.py`:

- `expected_dataset_names()` — the lake dataset name plus `wh_<table>` for
  every table in `source/models.py`'s `EXPECTED_COLUMNS`. Any dataset the
  pipeline actually produces that's missing from `catalog.json` is a
  failure.
- Every entry must have all required fields (`name`, `layer`,
  `description`, `owner`, `consumers`, `update_cadence`, `schema`) —
  non-empty.
- For every warehouse table, the catalog's documented schema must include
  every column `source/models.py` actually expects, plus `_cdc_seq` and
  `_deleted`. This specifically catches the catalog going stale after a
  schema change without anyone remembering to update `catalog.json` to
  match — a documentation file that can silently drift is worse than no
  documentation, so this is checked automatically rather than trusted.

`enforce()` raises `CatalogValidationError` listing every problem;
`__main__` catches it and exits non-zero, same pattern as the other two
validation scripts.
