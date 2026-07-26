# CDC Lakehouse — E-commerce Orders

Setup and run instructions. For the design rationale, trade-offs, and what's
been verified, see [`DESIGN.md`](./DESIGN.md) — this file is deliberately
just the practical steps.

## Prerequisites

- Python 3.12
- Docker (for Kafka and for Spark — Spark 4.0 needs Java 17, so it runs in
  a container rather than requiring a specific host Java version)
- A Kafka broker reachable from this machine (see "Kafka broker" below if
  you don't already have one running)

## 1. Python environment

```bash
cd submission/Shabbirsheikh
python3.12 -m venv .venv
source .venv/bin/activate
pip install duckdb==1.5.5 kafka-python==3.0.9 pyspark==4.0.0 pytest==9.1.1
```

## 2. Kafka broker

If you don't already have one running, a single-broker KRaft-mode instance
is enough:

```bash
docker run -d --name broker --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<your-host-ip>:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  apache/kafka:latest
```

Replace `<your-host-ip>` with an address other processes (including the
Spark container) can actually reach — `localhost` only works if every
component runs in the same network namespace as the broker.

Create the topic (single partition — see `DESIGN.md`'s CDC Strategy
section for why):

```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic cdc-events-v4 --partitions 1 --replication-factor 1
```

If you use a different bootstrap address or topic name, update
`BOOTSTRAP_SERVERS`/`TOPIC` in `pipeline/kafka_producer.py` and
`KAFKA_BOOTSTRAP`/`TOPIC` in `pipeline/spark_consumer.py` to match.

## 3. Spark image (for the consumer)

The Spark consumer runs inside a Docker image built on `apache/spark:4.0.0`
with the Kafka connector JARs it needs — `apache/spark`'s default image
doesn't ship a version of `kafka-clients` that matches what
`spark-sql-kafka-0-10` expects, so this rebuilds it with matching versions.

```dockerfile
# Dockerfile
FROM apache/spark:4.0.0

COPY jars/kafka-clients-4.0.0.jar /opt/spark/jars/
COPY jars/spark-sql-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/
COPY jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/
```

Download the three JARs above (matching versions for Spark 4.0.0 / Scala
2.13) into a `jars/` folder next to the Dockerfile, then:

```bash
docker build -t spark-kafka:4.0.0 .
```

You also need `kafka-clients-3.9.0.jar` and `commons-pool2-2.12.0.jar`
available on the host to mount over the pre-staged versions at run time
(see the run command below) — the versions baked into the base image don't
match what the Kafka connector actually expects.

## 4. Run the tests

```bash
cd submission/Shabbirsheikh
python -m pytest -q
```

21 tests, in-memory, no dependency on Kafka/Spark/any persisted file —
should complete in a couple of seconds.

## 5. Run the validation scripts

```bash
python scripts/check_schema_contracts.py    # source schema vs. expected contract
python scripts/run_data_quality_checks.py   # warehouse + lake vs. system/business rules
python scripts/validate_catalog.py          # catalog completeness vs. actual datasets
```

The data-quality and catalog scripts expect `data/warehouse.duckdb` and
`data/lake_parquet/` to already exist — run step 6 first if this is a
clean checkout.

## 6. Seed data and run the pipeline end to end

```bash
# writes to source.duckdb and publishes to Kafka
python scripts/seed_demo_data.py

# process what's in Kafka into the lake + warehouse (on-demand, default)
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v /etc/passwd:/etc/passwd:ro \
  -v /path/to/kafka-clients-3.9.0.jar:/opt/spark/jars/kafka-clients-4.0.0.jar:ro \
  -v /path/to/commons-pool2-2.12.0.jar:/opt/spark/jars/commons-pool2-2.12.0.jar:ro \
  -v "$(pwd)":/workspace -w /workspace \
  spark-kafka:4.0.0 /opt/spark/bin/spark-submit --master 'local[*]' pipeline/spark_consumer.py
```

To run continuously instead (polls Kafka every 5 seconds, stays running —
stop it with `docker stop` when you're done, it holds a resident JVM
process):

```bash
# append --continuous to the spark-submit line above
```

## 7. Verify a record end to end

```bash
duckdb data/source.duckdb -c "SELECT * FROM customers WHERE customer_id='c100';"
duckdb -c "SELECT * FROM read_parquet('data/lake_parquet/*/*.parquet') WHERE primary_key='c100';"
duckdb data/warehouse.duckdb -c "SELECT * FROM wh_customers WHERE customer_id='c100';"
```

## Project layout

```
source/models.py               source schema (DDL, expected-column contract)
pipeline/source_writer.py      writes to source.duckdb, then publishes to Kafka
pipeline/kafka_producer.py     Kafka producer / change-event envelope
pipeline/spark_consumer.py     Kafka -> Parquet lake + DuckDB warehouse
pipeline/warehouse.py          merge logic (apply_lake_events), time travel (reconstruct_as_of)
scripts/check_schema_contracts.py   schema-drift detection
scripts/run_data_quality_checks.py  system + business validation
scripts/validate_catalog.py         catalog completeness check
scripts/seed_demo_data.py           consistent demo dataset
catalog/catalog.json                dataset metadata (lake + warehouse)
tests/                               21 pytest tests, in-memory
feature.md, source.md, docs/         supporting documentation (see below)
```

`DESIGN.md` is the primary design document. `feature.md` maps each of the
assignment's six requirements to what was actually built; `source.md` has
the full data model and invariants; `docs/` has one file per component
(CDC/streaming, warehouse/time-travel, schema/data-quality, catalog) with
implementation-level detail.
