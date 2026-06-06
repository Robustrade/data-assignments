# Testing against a real PostgreSQL source (Docker)

This walks you through running the pipeline against a **real Postgres** instead of the
in-process simulator: create tables, insert/update/delete rows, and watch the changes
flow into the DuckDB **lake** (full history) and **warehouse** (current snapshot + time
travel).

The simulated path (`pytest`, `python -m ingestion.pipeline`) needs none of this — it
is fully covered without Postgres. This Postgres path is **optional** and not run in CI.

## Prerequisites

- Docker engine. On macOS without Docker Desktop you can use Colima:
  ```bash
  brew install colima docker docker-compose
  colima start
  ```
- Python deps (the Postgres driver `psycopg` is included in `requirements.txt`):
  ```bash
  cd submission/ecommerce-cdc
  python -m venv .venv && . .venv/bin/activate
  pip install -r requirements.txt
  ```

## 1. Start Postgres (auto-creates the schema)

```bash
docker-compose up -d
```

`docker-compose.yml` boots `postgres:16-alpine` and runs `source/postgres_ddl.sql` on
first start, so the 5 source tables + enums + indexes are created automatically.

## 2. See the tables

```bash
docker exec kulu_ecom_pg psql -U kulu -d kulu_ecom -c "\dt"
docker exec kulu_ecom_pg psql -U kulu -d kulu_ecom -c "\d orders"
```

Open an interactive SQL shell any time with:

```bash
docker exec -it kulu_ecom_pg psql -U kulu -d kulu_ecom
```

Connection details: host `localhost`, port `5432`, db `kulu_ecom`, user/pass `kulu`/`kulu`.

## 3. Insert rows

Either run the sample seeder:

```bash
python scripts/pg_seed.py
```

…or insert manually:

```bash
docker exec kulu_ecom_pg psql -U kulu -d kulu_ecom -c \
  "INSERT INTO customers (customer_id, name, email, status)
   VALUES ('c9','Dana','dana@example.com','active');"
```

## 4. Sync changes into the lake + warehouse

```bash
python scripts/pg_sync.py
```

This diffs Postgres against the previous sync (snapshot-diff CDC), writes every change
to the lake, refreshes the warehouse (current snapshot + SCD2 history), runs validation
parity, and prints a summary. Example after seeding:

```
=== sync ===
captured: 8 insert, 0 update, 0 delete (8 applied)
=== lake (full history) ===
8 events {'insert': 8}
=== warehouse current snapshot ===
  orders: [('o1', 'pending', Decimal('70.00'))]
=== validation parity ===
  all checks passed
```

## 5. Update / delete and re-sync (see CDC in action)

Use the batch mutation script (legal status transitions, a price change, a safe delete):

```bash
python scripts/pg_mutate.py
python scripts/pg_sync.py
```

…or do it by hand:

```bash
docker exec kulu_ecom_pg psql -U kulu -d kulu_ecom -c \
  "UPDATE orders SET status='paid', updated_at=now() WHERE order_id='o1';
   DELETE FROM customers WHERE customer_id='c2';"

python scripts/pg_sync.py
```

You'll see:
- the lake grows (now 10 events: 8 insert + 1 update + 1 delete) — **full history retained**
- `wh_orders` shows `o1` = `paid` — **latest snapshot**
- `wh_customers` live count drops by one — **soft delete**
- time travel prints the order's state at an earlier sequence vs now

## 6. Inspect the lakehouse directly (optional)

The lakehouse is a DuckDB file at `.data/lakehouse.duckdb`:

```bash
python - <<'PY'
import duckdb
c = duckdb.connect(".data/lakehouse.duckdb")
print(c.execute("SELECT sequence, operation, table_name, primary_key FROM lake_cdc_events ORDER BY sequence").fetchall())
print(c.execute("SELECT order_id, status, _deleted FROM wh_orders").fetchall())
print(c.execute("SELECT order_id, status, _valid_from_seq, _valid_to_seq, _is_current FROM wh_orders_history ORDER BY _valid_from_seq").fetchall())
PY
```

## How this maps to production

The `scripts/pg_sync.py` polling diff is the documented **simulated CDC**. In production
it is replaced by Debezium / Postgres logical replication reading the WAL; the sequence
becomes the LSN. Everything downstream — lake, warehouse, SCD2, validation, catalog —
stays exactly the same.

## Teardown

```bash
docker-compose down -v        # stop Postgres and remove its data volume
rm -rf .data                  # clear the local lakehouse file
colima stop                   # (optional) stop the Docker VM
```
