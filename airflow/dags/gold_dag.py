import os
import logging
from airflow import DAG, macros
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone


kst = timezone("Asia/Seoul")
TOTAL_FILE_COUNT_PER_DAILY = 20
# TOTAL_FILE_COUNT_PER_DAILY = 144


SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


def check_silver_file_count(**context):
    prev_date = macros.ds_add(context['ds'], -1)
    prefix = f"iceberg/silver/webtoon_user_session_events/data/datetime_day={prev_date}/"

    s3 = S3Hook(aws_conn_id="aws_default")
    files = s3.list_keys(bucket_name="w-userflow-featurestore", prefix=prefix)

    if not files:
        raise ValueError(f"No files under prefix : {prefix}")

    parquet_files = [f for f in files if f.endswith('.parquet')]
    count = len(parquet_files)

    logging.info(f"Found {count} parquet files under {prefix}")

    if count < TOTAL_FILE_COUNT_PER_DAILY:
        raise ValueError(f"Only {count}/{TOTAL_FILE_COUNT_PER_DAILY} parquet files found for {context['ds']} - silver not complete yet!")


with DAG(
    dag_id="gold_webtoon_episode_level_daily",
    start_date=datetime(2025, 10, 12, tzinfo=kst),
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["gold", "offline", "featurestore", "daily"]
) as dag:
    check_silver_data = PythonOperator(
        task_id="check_silver_data",
        python_callable=check_silver_file_count,
        provide_context=True
    )

    gold_content_daily_task = SparkSubmitOperator(
        task_id="gold_content_daily_task",
        application="/opt/workspace/src/spark/gold_webtoon_episode_level_daily.py",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}",
            # "--snapshot_date", "{{ ds }}"
        ],
        conf={
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "12g",
            "spark.driver.memory": "6g",
            "spark.cores.max": "2",
            "spark.sql.shuffle.partitions": "8",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.2",
        },
        verbose=True
    )

    check_silver_data >> gold_content_daily_task
