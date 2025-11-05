import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="bronze_webtoon_event_ingest",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 9, 27),
    catchup=False,
    tags=["bronze", "ingest", "iceberg", "kafka" ,"s3"]
) as dag:
    bronze_ingest_task = SparkSubmitOperator(
        task_id="bronze_load_raw_data",
        application="/opt/workspace/src/spark/bronze_load_raw_data.py",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        conf={
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "4g",
            "spark.cores.max": "1",
        },
        verbose=True
    )

    bronze_ingest_task