from __future__ import annotations

import duckdb

from pipeline.schema_contracts import validate_event_schema
from source.models import SCHEMA_CONTRACT, create_source_tables


def main() -> int:
    conn = duckdb.connect(":memory:")
    create_source_tables(conn)

    failures: list[str] = []
    for table in SCHEMA_CONTRACT:
        columns = {
            row[0]: row[1]
            for row in conn.execute(f"DESCRIBE {table}").fetchall()
        }
        failures.extend(
            f"{violation.table}: {violation.message}"
            for violation in validate_event_schema(table, columns)
        )

    if failures:
        for failure in failures:
            print(failure)
        return 1

    print("Schema contracts passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

