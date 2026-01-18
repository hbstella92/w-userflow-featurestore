import os
import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from pendulum import timezone
from datetime import datetime


kst = timezone("Asia/Seoul")


SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

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
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print(f"[Slack Alert] Sent successfully: {response.text}")
    except Exception as e:
        print(f"[Slack Alert] Failed to send : {e}")


def is_ancestor_snapshot(ss, table_name, start_id, end_id):
    current_id = end_id

    while True:
        df = ss.sql(f"""
            SELECT parent_id
            FROM {table_name}.snapshots
            WHERE snapshot_id = {current_id}
        """)
        rows = df.collect()

        if not rows or rows[0]["parent_id"] is None:
            break

        parent_id = rows[0]["parent_id"]
        if parent_id is None:
            return False

        if parent_id == start_id:
            return True

        current_id = parent_id

    return False


def get_snapshot_id(**context):
    from pyspark.sql import SparkSession

    ss = SparkSession.builder \
                    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
                    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
                    .config("spark.sql.catalog.iceberg.warehouse", SPARK_PARQUET_WAREHOUSE) \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .getOrCreate()
    
    snapshot_df = ss.sql("""
        SELECT snapshot_id
        FROM iceberg.bronze.webtoon_user_events_raw.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """)

    if snapshot_df.count() == 0:
        print(f"[SNAPSHOT] No snapshot found. Likely first run or empty table")
        raise AirflowFailException("[SNAPSHOT] No iceberg snapshot available. DAG will retry automatically")

    latest_snapshot_id = snapshot_df.collect()[0]["snapshot_id"]
    prev_snapshot_id = Variable.get("bronze_last_snapshot", default_var=None)

    print(f"[SNAPSHOT] prev snapshot id : {prev_snapshot_id}")
    print(f"[SNAPSHOT] latest snapshot id : {latest_snapshot_id}")

    if prev_snapshot_id and latest_snapshot_id:
        if not is_ancestor_snapshot(ss, "iceberg.bronze.webtoon_user_events_raw", int(prev_snapshot_id), int(latest_snapshot_id)):
            print("[SNAPSHOT] Lineage mismatch detected -> performing full read (start_snapshot_id = None)")
            prev_snapshot_id = None

    context["ti"].xcom_push(key="prev_snapshot_id", value=prev_snapshot_id)
    context["ti"].xcom_push(key="latest_snapshot_id", value=latest_snapshot_id)
    print("[SNAPSHOT] Snapshot check complete")


def update_snapshot_id(**context):
    latest_snapshot_id = context["ti"].xcom_pull(key="latest_snapshot_id")
    if latest_snapshot_id:
        Variable.set("bronze_last_snapshot", latest_snapshot_id)
        print(f"[Update] updated snapshot_id = {latest_snapshot_id}")


with DAG(
    dag_id="silver_user_session_events",
    default_args={
        "depends_on_past": False,
        "retries": 0
    },
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 9, 28, tzinfo=kst),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    on_failure_callback=slack_failure_alert,
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
        application_args=[
            "--date", "{{ ds }}",
            "--start_snapshot_id", "{{ ti.xcom_pull(task_ids='get_snapshot_id', key='prev_snapshot_id') | default('', true) }}",
            "--end_snapshot_id", "{{ ti.xcom_pull(task_ids='get_snapshot_id', key='latest_snapshot_id') }}"
        ],
        conf={
            # Spark setting
            "spark.local.dir": "/tmp/spark-tmp",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver": "python3",
            "spark.jars.ivy": "/opt/spark/.ivy2",
            "spark.executor.instances": "2",
            "spark.executor.cores": "2",
            "spark.executor.memory": "6g",
            "spark.driver.memory": "3g",
            "spark.cores.max": "4",
            "spark.sql.shuffle.partitions": "8",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.memory.fraction": "0.75",
            "spark.memory.storageFraction": "0.25",
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
        },
        verbose=True
    )

    update_snapshot_id_task = PythonOperator(
        task_id="update_snapshot_id",
        python_callable=update_snapshot_id,
        provide_context=True
    )

    get_snapshot_id_task >> silver_cleansing_task >> update_snapshot_id_task
