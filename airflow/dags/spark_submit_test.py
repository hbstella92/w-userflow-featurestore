import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


SPARK_APP_FILE_IN_CONTAINER = os.getenv("SPARK_APP_FILE_IN_CONTAINER")
SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")


with DAG(
    dag_id="spark_submit_test",
    start_date=datetime(2025, 9, 13),
    schedule_interval=None,
    catchup=False
) as dag:
    raw_file_load_task = SparkSubmitOperator(
        task_id="run_spark_submit_job",
        name="StreamingSessionJob",
        application=f"{SPARK_APP_FILE_IN_CONTAINER}",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        conf={
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver": "python3.11",
            "spark.jars.ivy": "/opt/bitnami/spark/.ivy2",
            # spark basic conf
            "spark.hadoop.fs.defaultFS": "file:///",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.sql.shuffle.partitions": "1",
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.cores": "2",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.cores.max": "2",
            # iceberg catalog setting
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            "spark.sql.catalog.iceberg.warehouse": "s3a://w-userflow-featurestore/iceberg/",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        },
        verbose=True
    )

    raw_file_load_task
