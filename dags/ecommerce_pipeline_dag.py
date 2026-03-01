"""
ecommerce_pipeline_dag.py — Airflow DAG that orchestrates the full pipeline.

DAG ID  : ecommerce_sales_pipeline
Schedule: @once  (trigger manually from the Airflow UI)

Tasks (sequential):
  1. setup_db    — Create/verify PostgreSQL star schema tables
  2. ingest_data — Read raw CSVs → write Parquet to bronze layer
  3. run_etl     — PySpark job: bronze → silver → PostgreSQL DW
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Absolute path to the project root (one directory above dags/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Callable wrappers for PythonOperators
# ---------------------------------------------------------------------------

def run_db_setup():
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.db_setup import load_config, setup_database
    cfg = load_config()
    setup_database(cfg)


def run_ingestion():
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.ingestion import load_config, ingest
    cfg = load_config()
    ingest(cfg)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data_engineering",
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_sales_pipeline",
    description="End-to-end Olist e-commerce sales pipeline (ingest → ETL → DW)",
    schedule_interval="@once",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "cm2606"],
) as dag:

    setup_db = PythonOperator(
        task_id="setup_db",
        python_callable=run_db_setup,
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=run_ingestion,
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=(
            f"spark-submit "
            f"--jars {PROJECT_ROOT}/jars/postgresql-42.7.3.jar "
            f"{PROJECT_ROOT}/scripts/etl_spark.py"
        ),
    )

    # Sequential dependency chain
    setup_db >> ingest_data >> run_etl
