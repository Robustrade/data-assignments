# Schema evolution and stop-the-line

The source offers no backward-compatibility guarantee. The pipeline therefore
assumes the schema will break, and is built to notice before it ingests
anything under a false assumption.

---

## The contract

`contracts/<table>.json` is the pipeline's published belief about one source
table: columns with declared type, nullability and enum domain; the primary
key; foreign keys; unique constraints; indexes; a version; an owner; a
description.

Contracts are **published artefacts**, not derived at runtime. The source is
hand-written and never reads them (see
[docs/00](00-architecture.md#why-the-source-is-hand-written-and-the-warehouse-spec-is-generated)),
which is what makes a comparison between the two meaningful.

`scripts/check_schema_contracts.py` proves in CI that the committed contracts
still describe a database built from the committed DDL.

---

## The compatibility matrix

Implemented in `cdclake/contracts.py:diff_table`. Applied to a live
introspection, never to intent.

| Change | Verdict | Reasoning |
|---|---|---|
| Add a column | **COMPATIBLE** (warn) | A reader is unaffected by a column it does not select. Warning, because the capture triggers must be regenerated before the new column reaches the lake. |
| Widen decimal precision, same scale | **COMPATIBLE** | Every existing value is still representable. |
| Add an enum value | **COMPATIBLE** (warn) | Ingestion is safe. Warning, because the warehouse domain assertion will start failing until the contract is republished — which is the correct order of events. |
| Remove an index | **COMPATIBLE** (warn) | Correctness is unaffected; extraction latency will regress. |
| Add a table | **COMPATIBLE** | Not under contract or capture yet. |
| Drop a column | **BREAKING** | Data the warehouse models depend on is gone. |
| Rename a column | **BREAKING** | See below. |
| Any other type change | **BREAKING** | Not value-preserving. |
| `nullable → NOT NULL` | **BREAKING** | Breaks writers and any backfill of historical rows. |
| `NOT NULL → nullable` | **BREAKING** | Breaks every downstream not-null assertion built on the column. |
| Remove an enum value | **BREAKING** | History in the lake still contains those values; their meaning has changed. |
| Change the primary key | **BREAKING** | Invalidates the merge key of every downstream model. |
| Change a foreign key | **BREAKING** | Referential assumptions have changed. |
| Change a unique constraint | **BREAKING** | Identity assumptions have changed. |
| Drop a contracted table | **BREAKING** | — |

### Two rules that are usually got wrong

**Both nullability directions are breaking.** The obvious rule — "tightening is
breaking, relaxing is fine" — is only half right. `nullable → NOT NULL` breaks
writers. `NOT NULL → nullable` breaks *readers*: every `not_null` assertion in
`models/schema.yml` was written on the strength of that guarantee, and the
first NULL will fail the warehouse build rather than the ingest, which is a much
worse place to find out. Both halt.
Pinned by `test_both_nullability_directions_are_breaking`.

**A rename is indistinguishable from a drop plus an add.** Without a mapping
hint from the source team there is no evidence that `full_name` *became*
`customer_name` rather than vanishing while an unrelated column appeared. The
gate detects the shape — exactly one column left, exactly one arrived, same
declared type — reports it as `COLUMN_RENAMED_OR_REPLACED`, says in the detail
that it cannot tell the difference, and halts. The operator supplies the
intent. Pinned by
`test_rename_is_reported_as_indistinguishable_from_drop_plus_add`.

---

## Stop the line

```
RUNNING ──breaking delta──▶ HALTED(reason, checkpoint, diff, detected_at)
HALTED  ──operator acknowledge──▶ RUNNING
```

The gate runs **before** the batch reads a single row, so nothing incompatible
can reach the lake. On a breaking verdict:

1. `_state/pipeline_state.json` is written with the reason, the checkpoint at
   which drift was detected, the timestamp, and the full classified diff.
2. An `ERROR` record is appended to `_state/alerts.ndjson` — the machine
   readable signal a webhook would carry in production. It is a file here
   precisely so tests can assert a warning was *surfaced*, not merely logged.
3. `IngestionHalted` is raised; the CLI exits non-zero.

Compatible drift takes the other branch: a `WARN` alert is emitted, the change
is logged, and ingestion continues.

### It stays stopped

`guard_not_halted` runs first in every batch. While the state file says
`HALTED`, the runner refuses to start at all — a scheduler cannot resume by
simply running again five minutes later. Getting out requires a human:

```bash
python -m cdclake status                    # what broke, and when
python -m cdclake republish-contracts       # adopt the live schema, bump version
python scripts/generate_dbt_entities.py     # propagate to the warehouse spec
python -m cdclake acknowledge --operator you --note "why this is safe"
```

`acknowledge` records who cleared it and why, in the state file and in the
alert stream. `republish-contracts` is deliberately a separate command:
accepting a breaking change is a decision about the *meaning* of the data, not
something a scheduler should make at 03:00.

The whole incident is executable:

```bash
make stop-the-line                     # drop_column
make stop-the-line MUTATION=rename_column
make stop-the-line MUTATION=remove_enum_value
```

---

## Proving the gate fires

A gate that has never been shown to fire is a comment, not a control.
`cdclake/source.py` ships eight schema mutations, each applied to a throwaway
database:

| Mutation | Expected |
|---|---|
| `drop_column` | BREAKING |
| `rename_column` | BREAKING |
| `change_type` | BREAKING |
| `nullable_to_not_null` | BREAKING |
| `remove_enum_value` | BREAKING |
| `add_nullable_column` | COMPATIBLE + warning |
| `add_enum_value` | COMPATIBLE + warning |
| `widen_type` | COMPATIBLE + warning |

`scripts/check_schema_contracts.py` asserts all eight classifications — and, for
the compatible ones, that a warning was actually produced. The same matrix runs
in pytest, parametrised, with the additional assertions that the checkpoint did
not move and no partial files were written.

Note that `change_type` also has to retire the type-dependent CHECK, because
`ROUND(unit_price, 2) = unit_price` cannot survive on a TEXT column. That is
not test scaffolding; it is why the change is breaking — the source loses a
guarantee the warehouse was relying on.

---

## Known limitation

Trigger-based capture cannot see a column the triggers were not written for. A
compatible column addition therefore produces a WARN telling the operator to
regenerate the capture triggers before the column reaches the lake; the
pipeline keeps running correctly on the columns it does know about in the
meantime. A WAL-based backend does not have this problem, which is one of the
reasons [ADR-0001](adr/0001-cdc-capture-mechanism.md) treats it as the
production target.
