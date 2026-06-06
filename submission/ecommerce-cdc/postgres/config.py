"""Connection/config for the optional Postgres source integration.

Defaults match docker-compose.yml; override with standard PG* env vars.
"""

from __future__ import annotations

import os

PG_CONN = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "user": os.environ.get("PGUSER", "kulu"),
    "password": os.environ.get("PGPASSWORD", "kulu"),
    "dbname": os.environ.get("PGDATABASE", "kulu_ecom"),
}

# Persistent DuckDB lakehouse file so lake/warehouse state survives across syncs.
_SUBMISSION_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKEHOUSE_PATH = os.environ.get(
    "LAKEHOUSE_DB", os.path.join(_SUBMISSION_ROOT, ".data", "lakehouse.duckdb")
)


def pg_dsn() -> str:
    c = PG_CONN
    return (
        f"host={c['host']} port={c['port']} user={c['user']} "
        f"password={c['password']} dbname={c['dbname']}"
    )
