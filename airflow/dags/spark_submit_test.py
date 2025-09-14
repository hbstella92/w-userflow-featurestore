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
    task = SparkSubmitOperator(
        task_id="run_spark_submit_job",
        name="StreamingSessionJob",
        application=f"{SPARK_APP_FILE_IN_CONTAINER}",
        conn_id="spark_default",
        master=f"{SPARK_MASTER}",
        packages=f"{SPARK_PACKAGES}",
        conf={
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver": "python3.11",
            "spark.jars.ivy": "/opt/bitnami/spark/.ivy2",
            "spark.hadoop.fs.defaultFS": "file:///",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.sql.shuffle.partitions": "1",
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g"
        },
        verbose=True
    )

    task