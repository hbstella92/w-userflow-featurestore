import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


SPARK_MASTER = os.getenv("SPARK_MASTER")
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
        application="/opt/workspace/src/spark/bronze/bronze_load_raw_data.py",
        conn_id="spark_default",
        conf={
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "4g",
            "spark.cores.max": "1",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
            "spark.sql.catalog.iceberg.warehouse": SPARK_PARQUET_WAREHOUSE,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.iceberg.s3.endpoint": "https://s3.ap-northeast-2.amazonaws.com",
            "spark.sql.catalog.iceberg.s3.region": os.getenv("AWS_REGION"),
            "spark.sql.catalog.iceberg.s3.path-style-access": "true",
            "spark.sql.catalog.iceberg.s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
            "spark.sql.catalog.iceberg.s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY")
        },
        verbose=True
    )

    bronze_ingest_task