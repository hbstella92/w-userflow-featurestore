import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR")

POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = "raw_user_events"

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("webtoon_id", StringType()),
    StructField("episode_id", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("local_timestamp", StringType()),
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

def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", POSTGRES_JDBC_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"[ERROR] Failed to sink batch {batch_id} : {e}")

if __name__ == "__main__":
    ss = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    # read from kafka
    df_raw = ss.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "5000") \
        .option("failOnDataLoss", "true") \
        .load()
    
    df = df_raw.selectExpr("CAST(value AS STRING) as v") \
        .select(from_json(col("v"), schema).alias("event")) \
        .select("event.*") \
        .withColumn("event_ts", to_timestamp("timestamp")) \
        .withColumn("event_local_ts", to_timestamp("local_timestamp")) \
        .withColumn("datetime", to_date("timestamp")) \
        .dropna(subset=["session_id", "event_ts"])
    
    # sink to parquet
    # query = df.writeStream \
    #     .format("parquet") \
    #     .option("path", "file:///opt/workspace/datalake/v1") \
    #     .option("checkpointLocation", f"{CHECKPOINT}/v1") \
    #     .partitionBy("datetime") \
    #     .outputMode("append") \
    #     .start()
    
    # read from parquet
    # df_2 = ss.readStream \
    #     .format("parquet") \
    #     .schema(schema) \
    #     .load("file:///opt/workspace/datalake/v1")
    
    # sink to postgres
    query2 = df.writeStream \
        .foreachBatch(write_to_postgres).start()

    # session processing
    windowed_df = df.withWatermark("event_ts", "1 minutes")

    features = windowed_df.groupBy("session_id", "user_id", "webtoon_id", "episode_id",
                            session_window("event_ts", "1 minutes").alias("window")) \
                                .agg(
                                    min("event_ts").alias("start_time"),
                                    max("event_ts").alias("end_time"),
                                    max("scroll_ratio").alias("max_scroll_ratio"),
                                    # sum("scroll_event_count").alias("scroll_events"),   # ???
                                    # sum("dwell_time_ms").alias("dwell_ms"), # ???
                                    max(col("event_type") == "enter").alias("seen_enter"),
                                    max(col("event_type") == "scroll").alias("seen_scroll"),
                                    max(col("event_type") == "complete").alias("seen_complete"),
                                    max(col("event_type") == "exit").alias("seen_exit")
                                ) \
                                .withColumn("is_complete",
                                            (col("seen_enter") & col("seen_scroll") & col("seen_complete") & (col("max_scroll_ratio") >= 1.0) & ((unix_timestamp("end_time") - unix_timestamp("start_time")) <= 300)).cast("int") \
                                ) \
                                .withColumn("is_exit",
                                            (col("seen_enter") & col("seen_scroll") & (col("seen_complete") == False) & (col("max_scroll_ratio") < 1.0)).cast("int") \
                                )
    
    query = features.writeStream \
        .format("console") \
        .option("checkpointLocation", "/tmp/checkpoints/v2") \
        .option("truncate", "false") \
        .outputMode("append") \
        .start()
    
    # query.awaitTermination()
    ss.streams.awaitAnyTermination()