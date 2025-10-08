import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


SPARK_MASTER = os.getenv("SPARK_MASTER")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")
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
        application="/opt/workspace/src/spark/bronze_load_raw_data.py",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        conf={
            "spark.local.dir": "/tmp/spark-tmp",
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver": "python3.11",
            # Spark setting
            "spark.jars.ivy": "/opt/spark/.ivy2",
            "spark.hadoop.fs.defaultFS": "s3a://w-userflow-featurestore/",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "4g",
            "spark.cores.max": "1",
            # AWS S3 setting
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            # Iceberg catalog setting
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",           # catalog name
            "spark.sql.catalog.iceberg.type": "hadoop",                                     # catalog backend type
            # "spark.sql.catalog.iceberg.type": "hive",
            # "spark.sql.catalog.iceberg.uri": "thrift://localhost:9083",
            "spark.sql.catalog.iceberg.warehouse": f"{SPARK_PARQUET_WAREHOUSE}",
        },
        verbose=True
    )

    bronze_ingest_task