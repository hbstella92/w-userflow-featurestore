import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pendulum import timezone


kst = timezone("Asia/Seoul")

SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

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

with DAG(
    dag_id="diagnosis_snapshot_check",
    start_date=datetime(2026, 5, 16, tzinfo=kst),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["diagnosis", "snapshot", "llm"],
) as dag:
    run_diagnosis = SparkSubmitOperator(
        task_id="run_diagnosis",
        application="/opt/workspace/src/diagnosis/run_diagnosis.py",
        conn_id="spark_default",
        conf=SPARK_APP_CONF,
        verbose=True,
    )
