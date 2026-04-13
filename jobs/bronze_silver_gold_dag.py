import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

username = os.getenv("USER_NAME", "hadoopnhung")
hadoop_master = os.getenv("HADOOP_MASTER_NAME", "hadoop-master")

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

    # Đường dẫn script 
    bronze_script = f"/home/{username}/jobs/bronze_to_silver_job.py"
    silver_script = f"/home/{username}/jobs/silver_processing.py"
    gold_script = f"/home/{username}/jobs/silver_to_gold_job.py"

    # Lệnh chạy Spark trên container Hadoop
    spark_submit_cmd = (
    f"docker exec -u {username} {hadoop_master} "
    "spark-submit --master yarn --deploy-mode cluster "
    )
    # Task 1: Bronze -> Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{spark_submit_cmd} {bronze_script}"
    )

    # Task 2: Silver processing
    silver_processing = BashOperator(
        task_id="silver_processing",
        bash_command=f"{spark_submit_cmd} {silver_script}"
    )

    # Task 3: Gold processing
    gold_processing = BashOperator(
        task_id="gold_processing",
        bash_command=f"{spark_submit_cmd} {gold_script}"
    )

    bronze_to_silver >> silver_processing >> gold_processing