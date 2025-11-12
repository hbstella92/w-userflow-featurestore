import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

SPARK_CHECKPOINT_DIR = "/tmp/"
SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


raw_event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("webtoon_id", StringType()),
    StructField("episode_id", StringType()),
    StructField("session_id", StringType()),
    StructField("utimestamptz", StringType()),
    StructField("local_timestamptz", StringType()),
    StructField("event_type", StringType()),
    StructField("country", StringType()),
    StructField("platform", StringType()),
    StructField("device", StringType()),
    StructField("browser", StringType()),
    StructField("network_type", StringType()),
    StructField("scroll_ratio", FloatType()),
    StructField("scroll_event_count", IntegerType()),
    StructField("dwell_time_ms", LongType())
])


if __name__ == "__main__":
    ss = SparkSession.builder \
                    .appName("BronzeLoadRawDataJob") \
                    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.iceberg.type", "hadoop") \
                    .config("spark.sql.catalog.iceberg.warehouse", f"{SPARK_PARQUET_WAREHOUSE}") \
                    .getOrCreate()
    
    ss.sparkContext.setLogLevel("WARN")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.webtoon_user_events_raw(
           event_id STRING,
           user_id INT,
           webtoon_id STRING,
           episode_id STRING,
           session_id STRING,
           utimestamptz STRING,
           local_timestamptz STRING,
           event_type STRING,
           country STRING,
           platform STRING,
           device STRING,
           browser STRING,
           network_type STRING,
           scroll_ratio DOUBLE,
           scroll_event_count INT,
           dwell_time_ms BIGINT,
           datetime DATE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    raw_df = ss.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("maxOffsetsPerTrigger", "5000") \
                .option("failOnDataLoss", "false") \
                .load()
    
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")
    df = json_df.select(from_json(col("json_str"), raw_event_schema).alias("data")).select("data.*")

    bronze_df = df.withColumn("datetime", to_date(col("utimestamptz")))
    print("[Data] Bronze table ex.")
    bronze_df.show(20, truncate=False)

    query = bronze_df.writeStream \
                    .format("iceberg") \
                    .outputMode("append") \
                    .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/bronze/1") \
                    .toTable("iceberg.bronze.webtoon_user_events_raw")
    
    query.awaitTermination()
