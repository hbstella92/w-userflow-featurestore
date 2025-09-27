import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")


with DAG(
    dag_id="batch_spark_job",
    start_date=datetime(2025, 9, 13),
    schedule_interval="@daily",
    catchup=False
) as dag:
    batch_completion_rate = SparkSubmitOperator(
        task_id="completion_rate_per_episode",
        name="BatchCompletionRatePerEpisode",
        application="/opt/workspace/src/spark/batch_completion_rate.py",
        application_args=["--date", "{{ ds }}"],
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
            "spark.executor.cores": "1",
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

    batch_completion_rate