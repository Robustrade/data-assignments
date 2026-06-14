# Databricks notebook source
dbutils.widgets.text("catalog", "robustrade_dev")
dbutils.widgets.text("volume_path", "/Volumes/robustrade_dev/audit/landing")

catalog = dbutils.widgets.get("catalog")
volume_path = dbutils.widgets.get("volume_path")

from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

bronze_table = f"{catalog}.bronze.cdc_events"
bronze = spark.table(bronze_table)

# COMMAND ----------

from dataclasses import dataclass
from typing import Iterable, List, Mapping


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str
    nullable: bool


@dataclass(frozen=True)
class ContractViolation:
    table: str
    message: str


def normalize_type(data_type: str) -> str:
    aliases = {
        "bigint": "long",
        "integer": "int",
        "varchar": "string",
        "datetime": "timestamp",
    }
    value = data_type.lower().strip()
    return aliases.get(value, value)


def validate_schema_contract(
    table: str,
    required_columns: Mapping[str, str],
    nullable_columns: Mapping[str, str],
    actual_columns: Iterable[ColumnSpec],
) -> List[ContractViolation]:
    actual = {column.name: column for column in actual_columns}
    violations = []

    for name, expected_type in required_columns.items():
        if name not in actual:
            violations.append(ContractViolation(table, f"required column missing: {name}"))
            continue

        column = actual[name]
        if normalize_type(column.data_type) != normalize_type(expected_type):
            violations.append(
                ContractViolation(
                    table,
                    f"type mismatch for {name}: expected {expected_type}, got {column.data_type}",
                )
            )
        if column.nullable:
            violations.append(ContractViolation(table, f"required column became nullable: {name}"))

    for name, expected_type in nullable_columns.items():
        if name not in actual:
            violations.append(ContractViolation(table, f"nullable column missing: {name}"))
            continue

        column = actual[name]
        if normalize_type(column.data_type) != normalize_type(expected_type):
            violations.append(
                ContractViolation(
                    table,
                    f"type mismatch for {name}: expected {expected_type}, got {column.data_type}",
                )
            )

    return violations


def stop_on_violations(violations: Iterable[ContractViolation]) -> None:
    messages = [f"{violation.table}: {violation.message}" for violation in violations]
    if messages:
        raise ValueError("Unsafe source schema change detected:\n" + "\n".join(messages))

# COMMAND ----------

payload_schemas = {
    "customers": T.StructType(
        [
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("email", T.StringType(), False),
            T.StructField("full_name", T.StringType(), False),
            T.StructField("phone_number", T.StringType(), True),
            T.StructField("kyc_level", T.StringType(), True),
            T.StructField("customer_status", T.StringType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "wallet_accounts": T.StructType(
        [
            T.StructField("wallet_id", T.StringType(), False),
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("currency", T.StringType(), False),
            T.StructField("balance_minor", T.LongType(), False),
            T.StructField("wallet_status", T.StringType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "merchants": T.StructType(
        [
            T.StructField("merchant_id", T.StringType(), False),
            T.StructField("merchant_name", T.StringType(), False),
            T.StructField("merchant_category", T.StringType(), False),
            T.StructField("merchant_status", T.StringType(), False),
            T.StructField("created_at", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "payment_transactions": T.StructType(
        [
            T.StructField("transaction_id", T.StringType(), False),
            T.StructField("wallet_id", T.StringType(), False),
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("merchant_id", T.StringType(), True),
            T.StructField("amount_minor", T.LongType(), False),
            T.StructField("currency", T.StringType(), False),
            T.StructField("transaction_status", T.StringType(), False),
            T.StructField("failure_code", T.StringType(), True),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
    "payment_attempts": T.StructType(
        [
            T.StructField("attempt_id", T.StringType(), False),
            T.StructField("transaction_id", T.StringType(), False),
            T.StructField("processor_name", T.StringType(), False),
            T.StructField("attempt_status", T.StringType(), False),
            T.StructField("failure_code", T.StringType(), True),
            T.StructField("attempted_at", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    ),
}

# COMMAND ----------

contracts = {
    "customers": {
        "required": {
            "customer_id": "string",
            "email": "string",
            "full_name": "string",
            "customer_status": "string",
            "updated_at": "timestamp",
        },
        "nullable": {"phone_number": "string", "kyc_level": "string"},
    },
    "wallet_accounts": {
        "required": {
            "wallet_id": "string",
            "customer_id": "string",
            "currency": "string",
            "balance_minor": "long",
            "wallet_status": "string",
            "updated_at": "timestamp",
        },
        "nullable": {},
    },
    "merchants": {
        "required": {
            "merchant_id": "string",
            "merchant_name": "string",
            "merchant_category": "string",
            "merchant_status": "string",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
        "nullable": {},
    },
    "payment_transactions": {
        "required": {
            "transaction_id": "string",
            "wallet_id": "string",
            "customer_id": "string",
            "amount_minor": "long",
            "currency": "string",
            "transaction_status": "string",
            "event_time": "timestamp",
            "updated_at": "timestamp",
        },
        "nullable": {"merchant_id": "string", "failure_code": "string"},
    },
    "payment_attempts": {
        "required": {
            "attempt_id": "string",
            "transaction_id": "string",
            "processor_name": "string",
            "attempt_status": "string",
            "attempted_at": "timestamp",
            "updated_at": "timestamp",
        },
        "nullable": {"failure_code": "string"},
    },
}

# COMMAND ----------

for table_name, schema in payload_schemas.items():
    actual_columns = [
        ColumnSpec(field.name, field.dataType.simpleString(), field.nullable)
        for field in schema.fields
    ]

    violations = validate_schema_contract(
        table_name,
        contracts[table_name]["required"],
        contracts[table_name]["nullable"],
        actual_columns,
    )

    stop_on_violations(violations)

print("All schema contracts passed.")

# COMMAND ----------

# bad_columns = [
#     ColumnSpec("amount_minor", "string", False)
# ]

# bad_violations = validate_schema_contract(
#     "payment_transactions",
#     {"amount_minor": "long"},
#     {},
#     bad_columns,
# )

# stop_on_violations(bad_violations)

# COMMAND ----------

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class TablePlan:
    source_table: str
    primary_key: str
    payload_columns: List[str]


plans = [
    TablePlan(
        source_table="customers",
        primary_key="customer_id",
        payload_columns=[
            "customer_id",
            "email",
            "full_name",
            "phone_number",
            "kyc_level",
            "customer_status",
            "updated_at",
        ],
    ),
    TablePlan(
        source_table="wallet_accounts",
        primary_key="wallet_id",
        payload_columns=[
            "wallet_id",
            "customer_id",
            "currency",
            "balance_minor",
            "wallet_status",
            "updated_at",
        ],
    ),
    TablePlan(
        source_table="merchants",
        primary_key="merchant_id",
        payload_columns=[
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "merchant_status",
            "created_at",
            "updated_at",
        ],
    ),
    TablePlan(
        source_table="payment_transactions",
        primary_key="transaction_id",
        payload_columns=[
            "transaction_id",
            "wallet_id",
            "customer_id",
            "merchant_id",
            "amount_minor",
            "currency",
            "transaction_status",
            "failure_code",
            "event_time",
            "updated_at",
        ],
    ),
    TablePlan(
        source_table="payment_attempts",
        primary_key="attempt_id",
        payload_columns=[
            "attempt_id",
            "transaction_id",
            "processor_name",
            "attempt_status",
            "failure_code",
            "attempted_at",
            "updated_at",
        ],
    ),
]

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.window import Window


def merge_condition(target_alias: str, source_alias: str, primary_key: str) -> str:
    return f"{target_alias}.{primary_key} = {source_alias}.{primary_key}"


def build_set_map(columns, source_alias: str = "s"):
    values = {column: f"{source_alias}.{column}" for column in columns}
    values["last_cdc_operation"] = f"{source_alias}.cdc_operation"
    values["last_cdc_sequence"] = f"{source_alias}.cdc_sequence"
    values["last_source_commit_ts"] = f"{source_alias}.source_commit_ts"
    values["silver_updated_at"] = "current_timestamp()"
    return values


def build_insert_map(columns, source_alias: str = "s"):
    return build_set_map(columns, source_alias)


def dedupe_latest_events(events_df, source_table: str, primary_key: str):
    filtered = events_df.where(F.col("source_table") == source_table)

    flattened = filtered.select(
        "source_table",
        "cdc_operation",
        "cdc_sequence",
        "source_commit_ts",
        "ingested_at",
        "payload.*",
    )

    window = Window.partitionBy(primary_key).orderBy(
        F.col("source_commit_ts").desc(),
        F.col("cdc_sequence").desc(),
    )

    return (
        flattened.withColumn("rn", F.row_number().over(window))
        .where(F.col("rn") == 1)
        .drop("rn")
        .withColumn("is_deleted", F.col("cdc_operation") == F.lit("DELETE"))
    )


def apply_cdc_merge(spark, events_df, target_table: str, plan: TablePlan):
    latest = dedupe_latest_events(events_df, plan.source_table, plan.primary_key)
    delta_table = DeltaTable.forName(spark, target_table)

    (
        delta_table.alias("t")
        .merge(latest.alias("s"), merge_condition("t", "s", plan.primary_key))
        .whenMatchedDelete(condition="s.is_deleted = true")
        .whenMatchedUpdate(
            condition="s.is_deleted = false AND s.cdc_sequence >= t.last_cdc_sequence",
            set=build_set_map(plan.payload_columns),
        )
        .whenNotMatchedInsert(
            condition="s.is_deleted = false",
            values=build_insert_map(plan.payload_columns),
        )
        .execute()
    )

# COMMAND ----------

for plan in plans:
    schema = payload_schemas[plan.source_table]

    parsed_events = bronze.withColumn(
        "payload",
        F.from_json("payload_json", schema),
    )

    empty_current = (
        parsed_events.where(F.col("source_table") == plan.source_table)
        .select("payload.*")
        .limit(0)
        .withColumn("last_cdc_operation", F.lit(None).cast("string"))
        .withColumn("last_cdc_sequence", F.lit(None).cast("long"))
        .withColumn("last_source_commit_ts", F.lit(None).cast("timestamp"))
        .withColumn("silver_updated_at", F.current_timestamp())
    )

    target_table = f"{catalog}.silver.{plan.source_table}_current"

    if not spark.catalog.tableExists(target_table):
        (
            empty_current.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        spark.sql(
            f"""
            ALTER TABLE {target_table} SET TBLPROPERTIES (
              'quality' = 'silver',
              'description' = 'Latest current-state table derived from CDC events'
            )
            """
        )

    apply_cdc_merge(spark, parsed_events, target_table, plan)

    print(f"Applied CDC merge into {target_table}")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.customers_current"))

# COMMAND ----------

display(spark.table(f"{catalog}.silver.wallet_accounts_current"))

# COMMAND ----------

display(spark.table(f"{catalog}.silver.merchants_current"))

# COMMAND ----------

display(spark.table(f"{catalog}.silver.payment_transactions_current"))

# COMMAND ----------

display(spark.table(f"{catalog}.silver.payment_attempts_current"))

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT transaction_id, transaction_status, last_cdc_sequence
        FROM {catalog}.silver.payment_transactions_current
        """
    )
)

# COMMAND ----------

for table_name in [
    "customers_current",
    "wallet_accounts_current",
    "merchants_current",
    "payment_transactions_current",
    "payment_attempts_current",
]:
    count = spark.table(f"{catalog}.silver.{table_name}").count()
    print(table_name, count)