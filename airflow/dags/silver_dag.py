import os
import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils import timezone
from datetime import datetime, timedelta


SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")

AIRFLOW__WEBSERVER__WEB_BASE_URL = os.getenv("AIRFLOW__WEBSERVER__WEB_BASE_URL")


def slack_failure_alert(context):
    try:
        conn = BaseHook.get_connection("slack_webhook")
        webhook_url = conn.password.strip()
    except Exception as e:
        print(f"[Slack Alert] Connection load failed : {e}")
        return

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    dag_run_id = context["dag_run"].run_id
    execution_date = context["execution_date"]
    try_number = context["task_instance"].try_number

    base_url = f"{AIRFLOW__WEBSERVER__WEB_BASE_URL}"
    log_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}&task_id={task_id}&map_index=-1&tab=logs"

    kst = timezone.convert_to_timezone(execution_date, "Asia/Seoul")

    message = (
        f"🚨 *Airflow DAG Failed!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"DAG : `{dag_id}`\n"
        f"Task : `{task_id}` (try {try_number})\n"
        f"Execution Time : {kst.strftime('%Y-%m-%d %H:%M:%S')} (KST)\n"
        f"<{log_url}|View Logs>"
    )

    try:
        response = requests.post(
            webhook_url, json={"text": message},
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print(f"[Slack Alert] Sent successfully: {response.text}")
    except Exception as e:
        print(f"[Slack Alert] Failed to send : {e}")


default_args = {
    "depends_on_past": False,
    "retries": 0,
    "on_failure_callback": slack_failure_alert
}


def get_snapshot_id(**context):
    from pyspark.sql import SparkSession
    ss = SparkSession.builder \
        .config("spark.jars.packages", f"{SPARK_PACKAGES}") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", f"{SPARK_PARQUET_WAREHOUSE}") \
        .getOrCreate()
    
    snapshot_df = ss.sql("""
        SELECT snapshot_id
        FROM iceberg.bronze.webtoon_user_events_raw.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """)
    snapshot_id = snapshot_df.collect()[0]["snapshot_id"]
    context["ti"].xcom_push(key="snapshot_id", value=str(snapshot_id))
    print(f"[Get] latest snapshot_id = {snapshot_id}")


def update_snapshot_id(**context):
    snapshot_id = context["ti"].xcom_pull(key="snapshot_id")
    Variable.set("bronze_last_snapshot", snapshot_id)
    print(f"[Update] updated snapshot_id = {snapshot_id}")


with DAG(
    dag_id="silver_user_session_events",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 9, 28),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "session", "cleansing", "iceberg", "spark"]
) as dag:
    get_snapshot_id_task = PythonOperator(
        task_id="get_snapshot_id",
        python_callable=get_snapshot_id,
        provide_context=True
    )

    silver_cleansing_task = SparkSubmitOperator(
        task_id="silver_user_session_events",
        application="/opt/workspace/src/spark/silver_user_session_events.py",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        application_args=[
            "--date", "{{ ds }}",
            "--start_snapshot_id", "{{ ti.xcom_pull(task_ids='get_snapshot_id', key='snapshot_id') }}"
        ],
        conf={
            "spark.local.dir": "/tmp/spark-tmp",
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver": "python3.11",
            # Spark setting
            "spark.jars.ivy": "/opt/spark/.ivy2",
            "spark.hadoop.fs.defaultFS": "s3a://w-userflow-featurestore/",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.cores": "2",
            "spark.executor.memory": "3g",
            "spark.driver.memory": "2g",
            "spark.executor.instances": "2",
            "spark.cores.max": "4",
            # AWS S3 setting
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            # Iceberg catalog setting
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            # "spark.sql.catalog.iceberg.type": "hive",
            # "spark.sql.catalog.iceberg.uri": "thrift://localhost:9083",
            "spark.sql.catalog.iceberg.warehouse": f"{SPARK_PARQUET_WAREHOUSE}"
        },
        verbose=True
    )

    update_snapshot_id_task = PythonOperator(
        task_id="update_snapshot_id",
        python_callable=update_snapshot_id,
        provide_context=True
    )

    get_snapshot_id_task >> silver_cleansing_task >> update_snapshot_id_task
