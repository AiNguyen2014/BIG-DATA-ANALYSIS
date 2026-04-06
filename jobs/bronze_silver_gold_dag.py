from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "bronze_silver_gold",
    default_args=default_args,
    description="DAG Bronze -> Silver -> Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 6),
    catchup=False
) as dag:

    # Bronze -> Silver Dataco
    bronze_to_silver_dataco = BashOperator(
        task_id="bronze_to_silver_dataco",
        bash_command="spark-submit /home/airflow/jobs/bronze_to_silver_dataco.py"
    )

    # Bronze -> Silver Logs
    bronze_to_silver_logs = BashOperator(
        task_id="bronze_to_silver_logs",
        bash_command="spark-submit /home/airflow/jobs/bronze_to_silver_logs.py"
    )

    # Silver -> Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="spark-submit /home/airflow/jobs/silver_to_gold.py"
    )

    # Thiết lập dependencies
    [bronze_to_silver_dataco, bronze_to_silver_logs] >> silver_to_gold