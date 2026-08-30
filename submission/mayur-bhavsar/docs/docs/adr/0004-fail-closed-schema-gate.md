# ADR-0004: Fail closed, and stay closed, on incompatible schema change

**Status:** accepted · **Date:** 2026-08-29

## Context

The source offers no backward-compatibility guarantee. When it changes
incompatibly, the pipeline has four options.

| Option | Behaviour |
|---|---|
| **A. Ignore** | Keep ingesting. Silent corruption, discovered weeks later by a confused analyst |
| **B. Warn and continue** | An alert nobody reads, and the corruption still happens |
| **C. Quarantine** | Route affected rows aside, keep the rest flowing |
| **D. Fail closed** | Stop ingesting; require a human decision to resume |

## Decision

**D**, implemented as a *persisted state machine* rather than an exception.

```
RUNNING ──breaking delta──▶ HALTED(reason, checkpoint, diff, detected_at)
HALTED  ──operator acknowledge──▶ RUNNING
```

The gate runs before the batch reads a single row. On a breaking verdict the
state file is written, an `ERROR` alert is appended, and `IngestionHalted` is
raised. `guard_not_halted` runs first in every subsequent batch, so **a
scheduled retry is refused** until a human clears it.

Compatible drift takes the other branch: `WARN` alert, ingestion continues.

## Consequences

**Good.** Nothing incompatible ever reaches the lake — the checkpoint does not
move and no partial files are written, both asserted for all five breaking
mutations. The blast radius of a source change is one halted pipeline and a
page, rather than a warehouse that has been quietly wrong since Tuesday.

**Good.** The halt survives process restart, because it is a file rather than
an in-memory flag. That is the property distinguishing a control from a log
line, and it is the one an exception-based design silently lacks.

**Good.** The exit is auditable. `acknowledge` records the operator and the
reason in both the state file and the alert stream.

**Bad, and accepted.** Ingestion stops entirely, including for tables that did
not change. A blast-radius-limited design would halt only the affected table
and let the rest flow. Rejected here because cross-entity assertions — `sum(line
items) = order total`, referential integrity — make a partially fresh warehouse
harder to reason about than a uniformly stale one. At a larger table count that
trade flips, and the gate is already per-table internally, so the change would
be local.

**Bad, and accepted.** A false positive stops a healthy pipeline. Mitigated by
the matrix being conservative in a *defined* way rather than a nervous one, and
by every classification being tested in both directions: five mutations must
halt, three must not.

## Why not quarantine (C)

Quarantine is the right answer for *bad rows* and the wrong one for a *broken
contract*. If a column has been dropped there is no subset of rows that is
still trustworthy — the models' assumptions are wrong for all of them.
Quarantine would produce a warehouse that is silently incomplete, which is
worse than one that is visibly stale.

Row-level quarantine is a genuine gap in the *quality* layer, though, and is
named as such in [docs/05](../05-validation-parity.md#gaps-worth-naming): a
failing assertion currently fails the whole build rather than routing the
offending rows aside.

## Break glass

Adopting a breaking change is a decision about the *meaning* of the data, so it
is a separate, deliberate command — never something the runner does on its own:

```bash
python -m cdclake republish-contracts       # adopt the live schema, bump version
python scripts/generate_dbt_entities.py     # propagate to the warehouse spec
python -m cdclake acknowledge --operator you --note "why this is safe"
```

`make stop-the-line` walks the entire incident and exits non-zero if any step
does not behave as documented.
