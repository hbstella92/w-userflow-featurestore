import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR")
SPARK_CHECKPOINT_DIR = "/tmp/"
SPARK_PARQUET_SILVER_PATH = os.getenv("SPARK_PARQUET_SILVER_PATH")


schema = StructType([
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


def write_to_parquet(batch_df, batch_id):
    try:
        print(f"[INFO] Batch {batch_id} input rows: {batch_df.count()}")
        batch_df.show(5, truncate=False)

        # batch_df.write \
        #     .mode("overwrite") \
        #     .option("partitionOverwriteMode", "dynamic") \
        #     .partitionBy("datetime") \
        #     .parquet(f"{SPARK_PARQUET_SILVER_PATH}")
        batch_df.writeTo("iceberg.silver.user_session_complete_or_exit") \
            .append()

        print(f"[INFO] Batch {batch_id} successfully written")
    except Exception as e:
        print("[ERROR] Failed to write parquet: ", e)


if __name__ == "__main__":
    ss = SparkSession.builder \
        .appName("StreamingSessionJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://w-userflow-featurestore/iceberg/") \
        .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    ss.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    ss.conf.set("spark.sql.sources.commitProtocolClass",
               "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    ss.conf.set("spark.sql.parquet.output.committer.class",
               "org.apache.parquet.hadoop.ParquetOutputCommitter")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.user_session_complete_or_exit (
           session_id STRING,
           user_id INT,
           webtoon_id STRING,
           episode_id STRING,
           platform STRING,
           country STRING,
           device STRING,
           browser STRING,

           window STRUCT<
            start TIMESTAMP,
            end TIMESTAMP
           >,

           start_time TIMESTAMP,
           end_time TIMESTAMP,
           duration_ms BIGINT,
           max_scroll_ratio DOUBLE,
           seen_enter BOOLEAN,
           seen_scroll BOOLEAN,
           seen_complete BOOLEAN,
           seen_exit BOOLEAN,

           datetime DATE,
           is_complete INT,
           is_exit INT
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    # read from kafka
    df_raw = ss.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "5000") \
        .option("failOnDataLoss", "false") \
        .load()

    # transformation
    df = df_raw.selectExpr("CAST(value AS STRING) as v") \
        .select(from_json(col("v"), schema).alias("event")) \
        .select("event.*") \
        .withColumn("event_ts", to_timestamp("utimestamptz")) \
        .withColumn("event_local_ts", to_timestamp("local_timestamptz")) \
        .withColumn("datetime", to_date("utimestamptz")) \
        .dropna(subset=["event_id", "session_id", "event_ts"])
    
    # session window aggregation
    windowed_df = df.withWatermark("event_ts", "30 seconds") \
        .groupby("session_id", "user_id", "webtoon_id", "episode_id",
                 "platform", "country", "device", "browser",
                 session_window("event_ts", "30 seconds").alias("window")) \
        .agg(
            min("event_ts").alias("start_time"),
            max("event_ts").alias("end_time"),
            max("dwell_time_ms").alias("duration_ms"),
            max("scroll_ratio").alias("max_scroll_ratio"),
            max(col("event_type") == "enter").alias("seen_enter"),
            max(col("event_type") == "scroll").alias("seen_scroll"),
            max(col("event_type") == "complete").alias("seen_complete"),
            max(col("event_type") == "exit").alias("seen_exit")
        ) \
        .withColumn("datetime", to_date("start_time")) \
        .withColumn("is_complete",
                    (col("seen_enter") & col("seen_scroll") & col("seen_complete") & (col("max_scroll_ratio") >= 1.0) & (unix_timestamp("end_time") - unix_timestamp("start_time") <= 300)).cast("int")
        ) \
        .withColumn("is_exit",
                    (col("seen_enter") & col("seen_scroll") & (col("seen_complete") == False) & (col("max_scroll_ratio") < 1.0) & (unix_timestamp("end_time") - unix_timestamp("start_time") <= 600)).cast("int")
        )
    
    query = windowed_df.writeStream \
        .foreachBatch(write_to_parquet) \
        .outputMode("append") \
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/19") \
        .trigger(processingTime="30 seconds") \
        .start() \
        .awaitTermination()
