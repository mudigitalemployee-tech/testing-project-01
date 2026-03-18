"""
sales_pipeline.py — Apache Airflow DAG
Batch ETL Pipeline for Sales Analytics
Schedule: Daily (@daily)

Tasks:
  1. check_file        — Verify superstore.csv exists
  2. load_to_staging   — Ingest CSV into stg_superstore
  3. data_validation   — Run DQ checks on staging
  4. transform_data    — Clean + derive columns
  5. load_fact_dim     — Load star schema (fact + dimensions)
  6. build_aggregates  — Build agg_sales_summary

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import os
import sys

# Add scripts to path so we can import pipeline modules
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "../scripts")
sys.path.insert(0, SCRIPTS_DIR)

log = logging.getLogger(__name__)

# ── Default Args ─────────────────────────────────────────────
default_args = {
    "owner": "ved-musigma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────
with DAG(
    dag_id="sales_pipeline",
    description="Batch ETL pipeline for Superstore Sales Analytics",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "sales", "superstore", "musigma"],
) as dag:

    # ── Task 1: check_file ──────────────────────────────────
    def check_file(**kwargs):
        csv_path = os.getenv("CSV_PATH", os.path.join(SCRIPTS_DIR, "../data/superstore.csv"))
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found at: {csv_path}")
        size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        log.info(f"File OK: {csv_path} ({size_mb:.2f} MB)")
        return {"path": csv_path, "size_mb": round(size_mb, 2)}

    t1_check_file = PythonOperator(
        task_id="check_file",
        python_callable=check_file,
        provide_context=True,
    )

    # ── Task 2: load_to_staging ─────────────────────────────
    def load_to_staging(**kwargs):
        from ingest import run
        count = run()
        log.info(f"Staged {count} rows")
        return count

    t2_load_staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
        provide_context=True,
    )

    # ── Task 3: data_validation ─────────────────────────────
    def data_validation(**kwargs):
        from validate import run_checks
        results = run_checks()
        log.info(f"DQ results: {results}")
        if not results.get("overall_pass"):
            raise ValueError("Data quality validation FAILED")
        return results

    t3_validate = PythonOperator(
        task_id="data_validation",
        python_callable=data_validation,
        provide_context=True,
    )

    # ── Task 4 + 5: transform_data + load_fact_dim ──────────
    def transform_and_load(**kwargs):
        from transform import run
        run()
        log.info("Transform + star schema load complete")

    t4_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_and_load,
        provide_context=True,
    )

    # ── Task 5 alias (aggregates are part of transform) ─────
    def build_aggregates_task(**kwargs):
        import psycopg2
        DB_CONN = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "dbname": os.getenv("DB_NAME", "salesdb"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
        }
        conn = psycopg2.connect(**DB_CONN)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM agg_sales_summary;")
                count = cur.fetchone()[0]
        conn.close()
        log.info(f"agg_sales_summary verified: {count} rows")
        return count

    t5_aggregates = PythonOperator(
        task_id="build_aggregates",
        python_callable=build_aggregates_task,
        provide_context=True,
    )

    # ── DAG Flow ─────────────────────────────────────────────
    # check_file → load_to_staging → data_validation → transform_data → build_aggregates
    t1_check_file >> t2_load_staging >> t3_validate >> t4_transform >> t5_aggregates
