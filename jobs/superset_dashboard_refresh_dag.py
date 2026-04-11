from datetime import datetime, timedelta
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

SUPERSET_URL = "http://superset-new:8088"
SUPERSET_USER = "admin"
SUPERSET_PASS = "admin"
DASHBOARD_ID = 1
DATASET_IDS = {
    "kpi_summary": 1,
    "monthly_financials": 2,
    "shipping_performance": 3,
    "customer_analytics": 4,
}

default_args = {
    "owner": "phan-D",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def get_token():
    res = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS,
              "provider": "db", "refresh": True},
        timeout=30,
    )
    return res.json()["access_token"]

def check_superset_health():
    res = requests.get(f"{SUPERSET_URL}/health", timeout=10)
    if res.status_code != 200:
        raise Exception(f"Superset unhealthy: {res.status_code}")
    logging.info("Superset health OK")

def refresh_datasets():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    for name, ds_id in DATASET_IDS.items():
        res = requests.put(
            f"{SUPERSET_URL}/api/v1/dataset/{ds_id}/refresh",
            headers=headers,
            timeout=30,
        )
        if res.status_code in (200, 201):
            logging.info(f"refresh {name}: OK")
        else:
            logging.warning(f"refresh {name}: {res.status_code} - {res.text}")

def warmup_dashboard_cache():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.put(
        f"{SUPERSET_URL}/api/v1/dashboard/{DASHBOARD_ID}/warm_up_cache",
        headers=headers,
        json={},
        timeout=60,
    )
    logging.info(f"warmup cache: {res.status_code}")

with DAG(
    dag_id="superset_dashboard_refresh",
    default_args=default_args,
    description="refresh Superset sau khi ETL xong",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 6),
    catchup=False,
    tags=["superset", "phan-D"],
) as dag:

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_bronze_silver_gold",
        external_dag_id="bronze_silver_gold",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(0),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    check_health = PythonOperator(
        task_id="check_superset_health",
        python_callable=check_superset_health,
    )

    refresh = PythonOperator(
        task_id="refresh_gold_datasets",
        python_callable=refresh_datasets,
    )

    warmup = PythonOperator(
        task_id="warmup_dashboard_cache",
        python_callable=warmup_dashboard_cache,
    )

    wait_for_etl >> check_health >> refresh >> warmup
