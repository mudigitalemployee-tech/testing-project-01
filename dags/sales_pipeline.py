"""
Airflow DAG: sales_pipeline (PRD 7)
Schedule: @daily | Retries: 2 (5-min delay)

DAG Flow (6 tasks):
  check_file → load_to_staging → data_validation →
  transform_data → load_fact_dim → build_aggregates → generate_report
"""
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import get_connection, load_config, execute_sql
from scripts.ingestion      import check_file, load_to_staging
from scripts.validate       import check_row_count, check_nulls, check_duplicates, check_sales_constraint
from scripts.transform      import clean, load_dim_date, load_dim_customer, load_dim_product, load_dim_region, load_fact_sales
from scripts.build_aggregates import run as run_aggregates
from scripts.generate_report  import generate_report
from utils.db import execute_sql_file
import pandas as pd

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_check_file(**ctx):
    config   = load_config()
    filepath = os.path.join(config["pipeline"]["data_dir"], config["pipeline"]["source_file"])
    check_file(filepath)
    ctx["ti"].xcom_push(key="filepath", value=filepath)


def task_load_to_staging(**ctx):
    config   = load_config()
    filepath = ctx["ti"].xcom_pull(key="filepath", task_ids="check_file")
    conn     = get_connection(config)
    try:
        rows = load_to_staging(filepath, conn)
        ctx["ti"].xcom_push(key="row_count", value=rows)
    finally:
        conn.close()


def task_data_validation(**ctx):
    config = load_config()
    conn   = get_connection(config)
    try:
        check_row_count(conn)
        check_nulls(conn)
        check_duplicates(conn)
        check_sales_constraint(conn)
    finally:
        conn.close()


def task_transform_data(**ctx):
    config = load_config()
    conn   = get_connection(config)
    try:
        sql_dir = os.path.join(os.path.dirname(__file__), "..", "sql")
        execute_sql_file(conn, os.path.join(sql_dir, "02_create_warehouse.sql"))
        rows = execute_sql(conn, "SELECT * FROM stg_superstore;", fetch=True)
        df   = pd.DataFrame([dict(r) for r in rows])
        df   = clean(df)
        load_dim_date(conn, df)
        load_dim_customer(conn, df)
        load_dim_product(conn, df)
        load_dim_region(conn, df)
        load_fact_sales(conn, df)
    finally:
        conn.close()


def task_build_aggregates(**ctx):
    run_aggregates()


def task_generate_report(**ctx):
    generate_report()


with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="Daily batch ETL pipeline for Superstore Sales Analytics",
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "etl", "superstore"],
) as dag:

    t1 = PythonOperator(task_id="check_file",        python_callable=task_check_file)
    t2 = PythonOperator(task_id="load_to_staging",   python_callable=task_load_to_staging)
    t3 = PythonOperator(task_id="data_validation",   python_callable=task_data_validation)
    t4 = PythonOperator(task_id="transform_data",    python_callable=task_transform_data)
    t5 = PythonOperator(task_id="load_fact_dim",     python_callable=task_transform_data)
    t6 = PythonOperator(task_id="build_aggregates",  python_callable=task_build_aggregates)
    t7 = PythonOperator(task_id="generate_report",   python_callable=task_generate_report)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
