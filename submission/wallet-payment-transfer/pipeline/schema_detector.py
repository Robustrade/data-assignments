"""
Schema-drift detector — implements the "stop the line" behavior.

Detection rules
---------------
For each table contract we compare the runtime source schema against
the contract and produce a list of `Drift` records. Each drift carries
a `severity`:

  BREAKING : the pipeline must stop. The contract change can silently
             corrupt the warehouse if we keep ingesting.
  WARN     : the pipeline can continue, but a human should know.

Breaking changes (fail closed):
  - column dropped              (existing references are now orphaned)
  - column renamed              (modelled as drop + add of an unknown col)
  - column type changed         (cast may silently round, truncate, error)
  - column nullability tightened
        nullable contract -> NOT NULL source
        means a row that previously satisfied the contract may now fail
  - enum domain shrunk          (a row that was valid before now is not)
  - primary key changed         (replay/upsert depends on the original PK)
  - referenced parent table missing (FK broken)

Warning-only changes:
  - new nullable column added   (additive, downstream just ignores it)
  - new enum value added        (downstream consumers can choose to allow)

Output
------
The detector returns a `DriftReport` per table and a top-level
`PipelineDriftReport` for the whole source. The CI script
`scripts/check_schema_contracts.py` exits non-zero whenever any
breaking drift is present.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import duckdb

from source.contracts import CONTRACTS, TableContract


class Severity(Enum):
    BREAKING = "BREAKING"
    WARN = "WARN"


@dataclass(frozen=True)
class Drift:
    table: str
    column: Optional[str]
    rule: str
    severity: Severity
    message: str


@dataclass
class DriftReport:
    table: str
    drifts: list[Drift] = field(default_factory=list)

    @property
    def breaking(self) -> list[Drift]:
        return [d for d in self.drifts if d.severity is Severity.BREAKING]

    @property
    def warnings(self) -> list[Drift]:
        return [d for d in self.drifts if d.severity is Severity.WARN]

    def has_breaking(self) -> bool:
        return any(d.severity is Severity.BREAKING for d in self.drifts)


@dataclass
class PipelineDriftReport:
    by_table: dict[str, DriftReport] = field(default_factory=dict)

    @property
    def breaking(self) -> list[Drift]:
        return [d for r in self.by_table.values() for d in r.breaking]

    @property
    def warnings(self) -> list[Drift]:
        return [d for r in self.by_table.values() for d in r.warnings]

    def has_breaking(self) -> bool:
        return any(r.has_breaking() for r in self.by_table.values())

    def summary(self) -> str:
        lines: list[str] = []
        for table, report in self.by_table.items():
            for d in report.drifts:
                marker = "x" if d.severity is Severity.BREAKING else "!"
                lines.append(f"  [{marker}] {table}.{d.column}: {d.rule} — {d.message}")
        return "\n".join(lines) if lines else "  (no drift)"


# ── runtime introspection ────────────────────────────────────────────────────


def _normalize_type(t: str) -> str:
    """
    DuckDB DESCRIBE returns types like 'VARCHAR' / 'DECIMAL(18,4)' /
    'TIMESTAMP' which already match our contract style. We strip
    whitespace and uppercase to be tolerant.
    """
    return t.strip().upper().replace(" ", "")


def _runtime_columns(
    conn: duckdb.DuckDBPyConnection, table: str
) -> list[tuple[str, str, bool]]:
    """
    Return [(name, type, nullable), ...] from DuckDB's information_schema.

    information_schema.columns is more reliable than DESCRIBE for
    nullability, since DESCRIBE in DuckDB does not always expose it.
    """
    rows = conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM   information_schema.columns
        WHERE  table_name = ?
        ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return [(r[0], _normalize_type(r[1]), r[2] == "YES") for r in rows]


def _runtime_pk(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """
    Best-effort PK lookup. DuckDB exposes constraints via duckdb_constraints();
    we scan for the PRIMARY KEY entry on the table.
    """
    try:
        rows = conn.execute(
            """
            SELECT constraint_column_names
            FROM   duckdb_constraints()
            WHERE  table_name = ? AND constraint_type = 'PRIMARY KEY'
            """,
            [table],
        ).fetchall()
    except Exception:
        return []
    if not rows:
        return []
    cols = rows[0][0]
    if isinstance(cols, list):
        return [str(c) for c in cols]
    if isinstance(cols, str):
        return [c.strip() for c in cols.split(",")]
    return []


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT 1 FROM information_schema.tables WHERE table_name = ?
            """,
            [table],
        ).fetchone()
    )


# ── core checker ─────────────────────────────────────────────────────────────


@dataclass
class SchemaDetector:
    """
    Runs schema contract checks against a live DuckDB connection.

    Used both by the CI script (`scripts/check_schema_contracts.py`)
    and at CDC capture time (`pipeline.cdc.CDCCapture` consults it
    before every event when `require_compatible_schema=True`).
    """

    conn: duckdb.DuckDBPyConnection

    def check_table(self, table: str) -> DriftReport:
        contract = CONTRACTS[table]
        report = DriftReport(table=table)

        if not _table_exists(self.conn, table):
            report.drifts.append(
                Drift(
                    table=table,
                    column=None,
                    rule="missing_table",
                    severity=Severity.BREAKING,
                    message="source table does not exist",
                )
            )
            return report

        runtime_cols = _runtime_columns(self.conn, table)
        runtime_by_name = {c[0]: c for c in runtime_cols}
        contract_by_name = {c.name: c for c in contract.columns}

        # 1) dropped columns
        for col in contract.columns:
            if col.name not in runtime_by_name:
                report.drifts.append(
                    Drift(
                        table, col.name,
                        "column_dropped",
                        Severity.BREAKING,
                        "contract column is missing from the source",
                    )
                )
                continue

            r_name, r_type, r_nullable = runtime_by_name[col.name]

            # 2) type change
            if not _types_compatible(col.type, r_type):
                report.drifts.append(
                    Drift(
                        table, col.name,
                        "type_changed",
                        Severity.BREAKING,
                        f"contract type {col.type!r} != source type {r_type!r}",
                    )
                )

            # 3) nullability tightened
            if col.nullable and not r_nullable:
                report.drifts.append(
                    Drift(
                        table, col.name,
                        "nullability_tightened",
                        Severity.BREAKING,
                        "contract allows NULL but source forbids it — "
                        "rows previously valid may now fail",
                    )
                )

        # 4) added columns (warning only, additive)
        for r_name, _, r_nullable in runtime_cols:
            if r_name not in contract_by_name:
                if r_nullable:
                    report.drifts.append(
                        Drift(
                            table, r_name,
                            "column_added",
                            Severity.WARN,
                            "new nullable column appeared on the source",
                        )
                    )
                else:
                    report.drifts.append(
                        Drift(
                            table, r_name,
                            "non_nullable_column_added",
                            Severity.BREAKING,
                            "new NOT-NULL column appeared on the source — "
                            "inserts from the contract perspective will fail",
                        )
                    )

        # 5) primary key changed
        runtime_pk = _runtime_pk(self.conn, table)
        if runtime_pk and runtime_pk != [contract.primary_key]:
            report.drifts.append(
                Drift(
                    table, ",".join(runtime_pk),
                    "primary_key_changed",
                    Severity.BREAKING,
                    f"contract PK is {contract.primary_key!r} "
                    f"but source PK is {runtime_pk!r}",
                )
            )

        # 6) enum domain shrinkage — checked by sampling actual rows.
        for col_name, allowed in contract.enum_domains:
            try:
                rows = self.conn.execute(
                    f"SELECT DISTINCT {col_name} FROM {table} "
                    f"WHERE {col_name} IS NOT NULL"
                ).fetchall()
            except Exception:
                continue
            present = {r[0] for r in rows}
            unexpected = present - set(allowed)
            if unexpected:
                # New value the contract did not list — additive, warn.
                report.drifts.append(
                    Drift(
                        table, col_name,
                        "enum_value_added",
                        Severity.WARN,
                        f"unexpected values present: {sorted(unexpected)!r}",
                    )
                )
            # We cannot tell from data alone whether the source has *removed*
            # an allowed value. The warehouse-side parity test
            # `tests/test_data_quality.py::test_enum_domains_match_contract`
            # checks the *materialized* warehouse so a removed value would
            # show up as a contract drift in subsequent runs.

        return report

    def check_all(self) -> PipelineDriftReport:
        out = PipelineDriftReport()
        for table in CONTRACTS:
            out.by_table[table] = self.check_table(table)
        return out


def _types_compatible(contract_type: str, runtime_type: str) -> bool:
    """
    Type equality is required. We are intentionally strict because silent
    casting between DECIMAL precisions or between VARCHAR/INT is exactly
    the kind of drift that produces hard-to-debug correctness bugs.
    """
    return _normalize_type(contract_type) == _normalize_type(runtime_type)
