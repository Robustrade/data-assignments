from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# =====================================================
# TASK WRAPPERS
# =====================================================

def validate_schema():

    from scripts.validate_schema import main

    main()


def ingest_cdc():

    from scripts.ingest_cdc import main

    main()


def build_silver():

    from scripts.build_silver import main

    main()


def build_warehouse():

    from scripts.build_warehouse import main

    main()


def run_data_quality():

    from scripts.run_data_quality_checks import main

    main()


def publish_catalog():

    from scripts.publish_catalog import main

    main()


# =====================================================
# DAG CONFIGURATION
# =====================================================

default_args = {

    "owner": "data-engineering",

    "depends_on_past": False,

    "email_on_failure": True,

    "email_on_retry": False,

    "retries": 1
}


# =====================================================
# DAG
# =====================================================

with DAG(

        dag_id="ecommerce_cdc_pipeline",

        description=(
            "CDC pipeline from PostgreSQL "
            "to Bronze, Silver and Warehouse"
        ),

        default_args=default_args,

        start_date=datetime(
            2026,
            1,
            1
        ),

        schedule="*/5 * * * *",

        catchup=False,

        tags=[
            "cdc",
            "spark",
            "warehouse"
        ]

) as dag:

    # ==========================================
    # 1. SCHEMA VALIDATION
    # ==========================================

    validate_schema_task = PythonOperator(

        task_id="validate_schema",

        python_callable=validate_schema
    )

    # ==========================================
    # 2. CDC INGESTION
    # ==========================================

    ingest_cdc_task = PythonOperator(

        task_id="ingest_cdc",

        python_callable=ingest_cdc
    )

    # ==========================================
    # 3. BUILD SILVER
    # ==========================================

    build_silver_task = PythonOperator(

        task_id="build_silver",

        python_callable=build_silver
    )

    # ==========================================
    # 4. BUILD WAREHOUSE
    # ==========================================

    build_warehouse_task = PythonOperator(

        task_id="build_warehouse",

        python_callable=build_warehouse
    )

    # ==========================================
    # 5. DATA QUALITY CHECKS
    # ==========================================

    data_quality_task = PythonOperator(

        task_id="run_data_quality_checks",

        python_callable=run_data_quality
    )

    # ==========================================
    # 6. PUBLISH CATALOG
    # ==========================================

    publish_catalog_task = PythonOperator(

        task_id="publish_catalog",

        python_callable=publish_catalog
    )

    # ==========================================
    # DEPENDENCIES
    # ==========================================

    (
        validate_schema_task
        >>
        ingest_cdc_task
        >>
        build_silver_task
        >>
        build_warehouse_task
        >>
        data_quality_task
        >>
        publish_catalog_task
    )