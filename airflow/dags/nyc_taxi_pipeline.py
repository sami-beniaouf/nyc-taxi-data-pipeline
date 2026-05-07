"""
nyc_taxi_pipeline.py

End-to-end NYC Taxi pipeline DAG.
Orchestrates Bronze ingestion → Silver transformation → Gold KPI aggregations.

Schedule: daily at 06:00 UTC (runs once per day in production)
For portfolio demo: trigger manually via Airflow UI
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Make pipeline modules importable from the dags folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline import bronze, silver, gold


# ---------------------------------------------------------------------------
# Paths — all local container paths (mounted from host via docker-compose)
# ---------------------------------------------------------------------------
BASE_PATH        = "/opt/airflow/data/final_project"
BRONZE_PATH      = f"{BASE_PATH}/bronze/yellow"
SILVER_PATH      = f"{BASE_PATH}/silver/yellow"
QUARANTINE_PATH  = f"{BASE_PATH}/quarantine/yellow"
GOLD_DAILY_PATH  = f"{BASE_PATH}/gold/daily_metrics"
GOLD_HOURLY_PATH = f"{BASE_PATH}/gold/hourly_patterns"
GOLD_ZONE_PATH   = f"{BASE_PATH}/gold/zone_performance"


# ---------------------------------------------------------------------------
# Task functions — thin wrappers that call pipeline modules
# ---------------------------------------------------------------------------
def run_bronze(**context):
    count = bronze.run(bronze_path=BRONZE_PATH)
    context["ti"].xcom_push(key="bronze_count", value=count)
    print(f"[DAG] Bronze complete — {count:,} rows")


def run_silver(**context):
    count = silver.run(
        bronze_path    = BRONZE_PATH,
        silver_path    = SILVER_PATH,
        quarantine_path= QUARANTINE_PATH,
    )
    context["ti"].xcom_push(key="silver_count", value=count)
    print(f"[DAG] Silver complete — {count:,} rows")


def run_gold(**context):
    counts = gold.run(
        silver_path      = SILVER_PATH,
        gold_daily_path  = GOLD_DAILY_PATH,
        gold_hourly_path = GOLD_HOURLY_PATH,
        gold_zone_path   = GOLD_ZONE_PATH,
    )
    context["ti"].xcom_push(key="gold_counts", value=counts)
    print(f"[DAG] Gold complete — {counts}")


def run_summary(**context):
    ti             = context["ti"]
    bronze_count   = ti.xcom_pull(task_ids="bronze_ingestion",       key="bronze_count")
    silver_count   = ti.xcom_pull(task_ids="silver_transformation",  key="silver_count")
    gold_counts    = ti.xcom_pull(task_ids="gold_aggregations",      key="gold_counts")

    print("=" * 50)
    print("NYC TAXI PIPELINE — RUN SUMMARY")
    print("=" * 50)
    print(f"  Bronze records  : {bronze_count:,}")
    print(f"  Silver records  : {silver_count:,}  ({silver_count/bronze_count*100:.2f}% retention)")
    print(f"  Gold tables     :")
    for name, cnt in gold_counts.items():
        print(f"    {name:20s}: {cnt} rows")
    print("=" * 50)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "sami-beniaouf",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id             = "nyc_taxi_pipeline",
    description        = "End-to-end NYC Taxi pipeline: Bronze → Silver → Gold",
    default_args       = default_args,
    start_date         = datetime(2024, 1, 1),
    schedule_interval  = "0 6 * * *",
    catchup            = False,
    tags               = ["nyc-taxi", "final-project", "medallion"],
) as dag:

    bronze_task = PythonOperator(
        task_id         = "bronze_ingestion",
        python_callable = run_bronze,
    )

    silver_task = PythonOperator(
        task_id         = "silver_transformation",
        python_callable = run_silver,
    )

    gold_task = PythonOperator(
        task_id         = "gold_aggregations",
        python_callable = run_gold,
    )

    summary_task = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = run_summary,
    )

    # Linear dependency: Bronze → Silver → Gold → Summary
    bronze_task >> silver_task >> gold_task >> summary_task
