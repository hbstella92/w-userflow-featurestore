import os
from airflow import DAG, macros
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone, now


kst = timezone("Asia/Seoul")
TOTAL_FILE_COUNT_PER_DAILY = 140


SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SPARK_APP_CONF = {
    # Spark setting
    "spark.local.dir": "/tmp/spark-tmp",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver": "python3",
    "spark.jars.ivy": "/opt/spark/.ivy2",
    "spark.executor.instances": "1",
    "spark.executor.cores": "2",
    "spark.executor.memory": "12g",
    "spark.driver.memory": "6g",
    "spark.cores.max": "2",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.2",

    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
    "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
    "spark.sql.catalog.iceberg.warehouse": SPARK_PARQUET_WAREHOUSE,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": "https://s3.ap-northeast-2.amazonaws.com",
    "spark.sql.catalog.iceberg.s3.region": "ap-northeast-2",
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.s3.access-key-id": AWS_ACCESS_KEY_ID,
    "spark.sql.catalog.iceberg.s3.secret-access-key": AWS_SECRET_ACCESS_KEY
}


def check_silver_file_count(**context):
    prev_date = macros.ds_add(context["ds"], -1)
    # prev_date = context['ds']
    prefix = f"iceberg/silver/webtoon_user_session_events/data/datetime_day={prev_date}/"

    s3 = S3Hook(aws_conn_id="aws_default")
    files = s3.list_keys(bucket_name="w-userflow-featurestore", prefix=prefix)

    if not files:
        raise ValueError(f"No files under prefix : {prefix}")

    parquet_files = [f for f in files if f.endswith(".parquet")]
    count = len(parquet_files)

    if count < TOTAL_FILE_COUNT_PER_DAILY:
        raise ValueError(f"Only {count} / {TOTAL_FILE_COUNT_PER_DAILY} parquet files found for {context['ds']} - silver not complete yet!")


with DAG (
    dag_id="gold_user_daily_metrics",
    start_date=datetime(2025, 11, 15, tzinfo=kst),
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["gold", "offline", "user", "daily"]
) as dag:
    check_silver_data = PythonOperator(
        task_id="check_silver_data",
        python_callable=check_silver_file_count,
        provide_context=True
    )

    gold_user_daily_metrics = SparkSubmitOperator(
        task_id="gold_user_daily_metrics",
        application="/opt/workspace/src/spark/gold_user_daily_metrics.py",
        conn_id="spark_default",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            # "--snapshot_date", "{{ ds }}"
        ],
        conf=SPARK_APP_CONF,
        verbose=True
    )

    gold_webtoon_episode_daily_metrics = SparkSubmitOperator(
        task_id="gold_webtoon_episode_daily_metrics",
        application="/opt/workspace/src/spark/gold_webtoon_episode_daily_metrics.py",
        conn_id="spark_default",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            # "--snapshot_date", "{{ ds }}"
        ],
        conf=SPARK_APP_CONF,
        verbose=True
    )

    gold_webtoon_daily_metrics = SparkSubmitOperator(
        task_id="gold_webtoon_daily_metrics",
        application="/opt/workspace/src/spark/gold_webtoon_daily_metrics.py",
        conn_id="spark_default",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            # "--snapshot_date", "{{ ds }}"
        ],
        conf=SPARK_APP_CONF,
        verbose=True
    )

    gold_platform_device_daily_metrics = SparkSubmitOperator(
        task_id="gold_platform_device_daily_metrics",
        application="/opt/workspace/src/spark/gold_platform_device_daily_metrics.py",
        conn_id="spark_default",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            # "--snapshot_date", "{{ ds }}"
        ],
        conf=SPARK_APP_CONF,
        verbose=True
    )

    gold_country_daily_metrics = SparkSubmitOperator(
        task_id="gold_country_daily_metrics",
        application="/opt/workspace/src/spark/gold_country_daily_metrics.py",
        conn_id="spark_default",
        application_args=[
            "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            # "--snapshot_date", "{{ ds }}"
        ],
        conf=SPARK_APP_CONF,
        verbose=True
    )

    check_silver_data >> gold_user_daily_metrics >> gold_webtoon_episode_daily_metrics >> gold_webtoon_daily_metrics >> gold_platform_device_daily_metrics >> gold_country_daily_metrics
