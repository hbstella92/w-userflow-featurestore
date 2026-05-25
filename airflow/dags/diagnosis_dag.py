import os
import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from pendulum import timezone


kst = timezone("Asia/Seoul")

SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AIRFLOW__WEBSERVER__WEB_BASE_URL = os.getenv("AIRFLOW__WEBSERVER__WEB_BASE_URL")


def _get_slack_webhook_url() -> str:
    try:
        conn = BaseHook.get_connection("slack_webhook")
        return conn.password.strip()
    except Exception:
        return ""


SPARK_APP_CONF = {
    "spark.local.dir": "/tmp/spark-tmp",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver": "python3",
    "spark.jars.ivy": "/opt/spark/.ivy2",
    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.cores.max": "1",

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
    "spark.sql.catalog.iceberg.s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
}


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

    base_url = AIRFLOW__WEBSERVER__WEB_BASE_URL or ""
    log_url = f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}&task_id={task_id}&map_index=-1&tab=logs"

    kst_time = execution_date.in_timezone(kst)

    message = (
        f"🚨 *Airflow DAG Failed!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"DAG : `{dag_id}`\n"
        f"Task : `{task_id}` (try {try_number})\n"
        f"Execution Time : {kst_time.strftime('%Y-%m-%d %H:%M:%S')} (KST)\n"
        f"<{log_url}|View Logs>"
    )

    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        print(f"[Slack Alert] Sent successfully: {response.text}")
    except Exception as e:
        print(f"[Slack Alert] Failed to send : {e}")


with DAG(
    dag_id="diagnosis_snapshot_check",
    start_date=datetime(2026, 5, 16, tzinfo=kst),
    schedule_interval="*/10 * * * *",
    catchup=False,
    on_failure_callback=slack_failure_alert,
    tags=["diagnosis", "snapshot", "llm"],
) as dag:
    run_diagnosis = SparkSubmitOperator(
        task_id="run_diagnosis",
        application="/opt/workspace/src/diagnosis/run_diagnosis.py",
        conn_id="spark_default",
        conf=SPARK_APP_CONF,
        env_vars={
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
            "SLACK_WEBHOOK_URL": _get_slack_webhook_url(),
        },
        verbose=True,
    )
