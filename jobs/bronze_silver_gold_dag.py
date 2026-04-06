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

    bronze_script = "/home/hadoopnhung/jobs/bronze_to_silver_job.py"
    silver_script = "/home/hadoopnhung/jobs/silver_processing.py"
    gold_script = "/home/hadoopnhung/jobs/gold_processing.py"

    # Base Spark submit command với Delta options
    spark_submit_cmd = (
        "spark-submit "
        "--deploy-mode cluster "
        "--packages io.delta:delta-spark_2.13:4.0.0 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    )

    # Task 1: Bronze → Silver (load raw data)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{spark_submit_cmd} {bronze_script}"
    )

    # Task 2: Silver processing (clean + standardize)
    silver_processing = BashOperator(
        task_id="silver_processing",
        bash_command=f"{spark_submit_cmd} {silver_script}"
    )

    # Task 3: Gold processing (aggregation / analytics)
    gold_processing = BashOperator(
        task_id="gold_processing",
        bash_command=f"{spark_submit_cmd} {gold_script}"
    )

    # Dependencies:
    bronze_to_silver >> silver_processing >> gold_processing