import os
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.avro.functions import from_avro

APP_NAME = os.getenv("SPARK_APP_NAME")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_DIR")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
AVSC_PATH = os.getenv("AVSC_PATH")

POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
TARGET_TABLE = os.getenv("POSTGRES_TABLE")


@F.udf(returnType=T.BinaryType())
def strip_sr_header(v: bytes):
    if v is None:
        return None
    
    return v[5:] if len(v) > 5 and v[0] == 0 else v


def write_to_pg(df, epoch_id: int):
    if df.rdd.isEmpty():
        return
    
    df.select(
        "user_id", "webtoon_id", "episode_id", "session_id",
        "timestamp", "local_timestamp", "event_type", "country", "platform",
        "device", "browser", "network_type", "scroll_ratio", "scroll_event_count",
        "dwell_time_ms"
    ).write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", TARGET_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .option("checkpointLocation", CHECKPOINT) \
    .mode("append") \
    .save()


if __name__ == "__main__":
    with open(AVSC_PATH, "r") as file:
        AVRO_SCHEMA = file.read()

    ss = SparkSession.builder.appName(APP_NAME).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    # 1. Reading from kafka
    raw_events = ss.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
        # .option("kafka.security.protocol", "PLAINTEXT") \
        # .load()
    
    # 2. Striping 5 bytes (Schema Registry header) and Decoding
    payload = strip_sr_header(F.col("value"))
    events = raw_events.select(from_avro(payload, AVRO_SCHEMA).alias("e")).select("e.*")

    # 3. Casting column and Normalizing
    events = events \
        .withColumn("timestamp", F.to_timestamp("timestamp")) \
        .withColumn("local_timestamp", F.to_timestamp("local_timestamp"))
    
    # 4. Loading data on PostgreSQL
    query = events.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT) \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .start()
    
    query.awaitTermination()