"""Optional PostgreSQL source integration (requires psycopg + a running Postgres).

This package lets you exercise the pipeline against a real Postgres source instead of
the in-process simulator: insert/update/delete rows in Postgres, then run a sync that
captures the changes (snapshot-diff CDC) into the DuckDB lake + warehouse.
"""
