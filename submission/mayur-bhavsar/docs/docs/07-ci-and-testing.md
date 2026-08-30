# CI and testing

## The seven required checks

The repository's workflow detects the changed `submission/<candidate>/` folder
and runs everything inside it. Each job maps onto something real here.

| CI job | Runs | Here |
|---|---|---|
| `lint-python-or-sql` | `ruff check`, `black --check`, `sqlfluff lint` | Clean. Config in `pyproject.toml` and `.sqlfluff` |
| `test-pipeline` | `pytest -q` | 89 tests, ~45s |
| `validate-models` | `dbt parse`, `dbt test` | 128 assertions — see the bootstrap note below |
| `schema-contract-check` | `scripts/check_schema_contracts.py` | Contracts vs DDL, generated spec currency, all 8 mutations |
| `data-quality-check` | `scripts/run_data_quality_checks.py` | 87 platform checks + a full `dbt build` |
| `catalog-check` | `scripts/validate_catalog.py` | 4 checks, one against a live warehouse |
| `security-and-secrets-scan` | `pip-audit`, `bandit`, `gitleaks` | No secrets; no credentials anywhere in the tree |

`make checks` runs the same set locally, in the same order.

---

## The `dbt test` bootstrap, and why it exists

The `validate-models` job runs `dbt parse` and then `dbt test`, with **no
`dbt run` between them** and no pipeline execution beforehand. Left alone,
`dbt test` fails on missing relations.

The tempting fix is to delete `dbt_project.yml`, at which point the job prints
"skipping" and goes green. That makes the check vacuous, which is worse than
failing it.

Instead, the `on-run-start` hook (`macros/bootstrap.sql`) materialises the
warehouse from a committed lake extract — `warehouse/fixtures/lake_cdc_events.ndjson`,
46 real change records produced by `make fixture` — using **exactly the macros
the models use**:

```jinja
{% macro bootstrap_warehouse() %}
    {% if execute %}
        {% do run_query(build_lake_relation_sql()) %}
        {% if flags.WHICH == "test" %}
            {% do bootstrap_models() %}
        {% endif %}
    {% endif %}
    select 1 as bootstrap_ok
{% endmacro %}
```

Two details make this safe rather than clever:

- **Model SQL lives in `macros/warehouse.sql`, not inline in the models.** Both
  the models (passing `ref()` relations) and the bootstrap (passing plain
  relation names) call the same macros, so the two paths cannot drift. The
  model files are one-line wrappers.
- **The models are only bootstrapped for `dbt test`** (`flags.WHICH == "test"`).
  For `dbt run` and `dbt build` dbt materialises them itself moments later; the
  hook only rebuilds the lake relation, which is the ingest boundary.

The fixture is a committed artefact of the pipeline, refreshed with
`make fixture`. It is text, so a reviewer can read it.

---

## The pytest suite

89 tests, ~45 seconds. Building the warehouse costs a few seconds, so the
read-only warehouse tests share one session-scoped reference run; tests that
interfere with ingestion get their own throwaway platform.

| File | Tests | Covers |
|---|---|---|
| `test_source_constraints.py` | 13 | Every source constraint, asserted by trying to violate it. Weak-entity keys, index presence, contract completeness |
| `test_cdc_capture.py` | 9 | Insert/update/delete capture, before/after images, cascaded child deletes, composite keys, event ordering, manifest and metrics |
| `test_replay_and_recovery.py` | 8 | No-op reruns, restart from checkpoint, crash before manifest, self-healing retry, orphan GC, duplicate absorption, out-of-order arrival, full replay from zero |
| `test_schema_gate.py` | 21 | Each breaking mutation halts / persists / alerts (5 × 3 parametrised), refused restart, acknowledgement, break-glass republish, each compatible mutation warns and continues, rename detection, both nullability directions |
| `test_warehouse.py` | 11 | Reconciliation, tombstones present in history and absent from current, row-by-row equality with the source, price change not rewriting history, interval chaining, exact decimals, and that no model reads the source |
| `test_time_travel.py` | 8 | Restore by version and by timestamp, before creation, past a delete, weak entities alongside parents, as-of-now equals current, deterministic collision resolution, restore written back to the source |
| `test_validation_parity.py` | 7 | Parity passes, covers every table — and **fails** when a `not_null`, an enum domain, a `relationships` or a `unique` is removed |
| `test_catalog.py` | 9 | Both layers present, metadata complete, schemas match the live warehouse, lineage resolves, PII flagged — and both failure directions detected |

### Tests that can fail

The parity checker and the catalog validator are both tested for their ability
to *fail*, by mutating a copy of the input and asserting the specific finding.
A check that only ever passes proves nothing about the thing it claims to
guard.

Likewise the schema gate: `scripts/check_schema_contracts.py` applies all eight
mutations and asserts each classification, including that the compatible ones
still produce a warning.

---

## Red, Blue, Green

The workflow was followed per behaviour rather than per line: write the failing
assertion, make it pass minimally, then refactor. Two cases where the discipline
changed the design rather than just the code, both preserved in the current
tests:

**Out-of-order arrival.** The first version of the SCD2 macro windowed by
`committed_at`. `test_out_of_order_arrival_produces_the_same_warehouse` — which
reverses the record order before writing — failed. The fix was to window by
`_version`, which is what makes the "pure function of the lake" property true
rather than aspirational.

**Point-in-time restore.** `test_a_deleted_entity_can_be_recovered_from_before_the_delete`
originally used `tombstone.valid_from - 1ms` and was intermittently red: the
cancellation and the delete sometimes committed inside the same millisecond, so
subtracting a millisecond landed before *both*. The honest fix was not to widen
the tolerance but to add `as_of_version` — a version-exact restore primitive —
and to document that a timestamp cannot separate two changes in the same
millisecond. That flake surfaced a real limitation of timestamp-only time
travel, and the design is better for having hit it.

Other tests were written first and passed immediately; those are not claimed as
red-green cycles.

---

## Running it

```bash
make test               # pytest
make quality            # platform checks + dbt build
make contracts          # schema contract check
make catalog            # catalog validation
make lint               # ruff + black
make checks             # all of the above, in CI's order

make demo               # the happy path, narrated
make stop-the-line      # a breaking schema change, narrated
make crash-recovery     # a crash mid-batch, narrated
```

The three `make` demos exit non-zero if the documented behaviour does not
happen, so they are executable specifications as well as demonstrations.
