import os
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from pendulum import timezone


kst = timezone("Asia/Seoul")


SPARK_PACKAGES = os.getenv("SPARK_PACKAGES")
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


with DAG(
    dag_id="gold_webtoon_episode_level_daily",
    # schedule_interval="*/10 * * * *",
    schedule_interval=None,
    start_date=datetime(2025, 10, 6, tzinfo=kst),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    tags=["gold", "featurestore", "daily"]
) as dag:
    # wait_for_silver_task = ExternalTaskSensor(
    #     task_id="wait_for_silver_task",
    #     external_dag_id="silver_user_session_events",
    #     external_task_id="update_snapshot_id",
    #     # execution_delta=timedelta(hours=3),
    #     mode="poke",
    #     poke_interval=60,
    #     timeout=3600
    # )

    gold_content_daily_task = SparkSubmitOperator(
        task_id="gold_content_daily_task",
        application="/opt/workspace/src/spark/gold_webtoon_episode_level_daily.py",
        conn_id="spark_default",
        packages=f"{SPARK_PACKAGES}",
        application_args=[
            # "--snapshot_date", "{{ macros.ds_add(ds, -1) }}"
            "--snapshot_date", "{{ ds }}"
        ],
        conf={
            # Spark setting
            "spark.local.dir": "/tmp/spark-tmp",
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver": "python3.11",
            "spark.jars.ivy": "/opt/spark/.ivy2",
            "spark.driver.extraJavaOptions": "-Duser.name=spark",
            "spark.executor.extraJavaOptions": "-Duser.name=spark",
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
            # AWS S3 setting
            "spark.hadoop.fs.defaultFS": "s3a://w-userflow-featurestore/",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.connection.maximum": "200",
            # Iceberg catalog setting
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            # "spark.sql.catalog.iceberg.type": "hive",
            # "spark.sql.catalog.iceberg.uri": "thrift://localhost:9083",
            "spark.sql.catalog.iceberg.warehouse": f"{SPARK_PARQUET_WAREHOUSE}",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
    )

    # wait_for_silver_task >> gold_content_daily_task
    gold_content_daily_task