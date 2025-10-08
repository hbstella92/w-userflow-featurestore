import os
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from datetime import datetime


SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


def to_int_or_none(x):
    try:
        return int(x) if x not in (None, "", "None") else None
    except ValueError:
        return None


if __name__ == "__main__":
    ss = SparkSession.builder \
        .appName("SilverUserSessionEventsJob") \
        .getOrCreate()
    
    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.webtoon_user_session_events(
           session_id STRING,
           user_id INT,
           webtoon_id STRING,
           episode_id STRING,
           platform STRING,
           country STRING,
           device STRING,
           browser STRING,
           datetime DATE,
           window STRUCT<
            start TIMESTAMP,
            end TIMESTAMP
           >,
           start_time TIMESTAMP,
           end_time TIMESTAMP,
           duration_ms BIGINT,
           avg_scroll_ratio DOUBLE,
           max_scroll_ratio DOUBLE,
           seen_enter BOOLEAN,
           seen_scroll BOOLEAN,
           seen_complete BOOLEAN,
           seen_exit BOOLEAN,
           
           is_complete INT,
           is_exit INT
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--start_snapshot_id", required=False)
    parser.add_argument("--end_snapshot_id", required=True)
    args = parser.parse_args()

    start_snapshot_id = to_int_or_none(args.start_snapshot_id)
    end_snapshot_id = to_int_or_none(args.end_snapshot_id)
    print(f"[START SNAPSHOT ID]\n{start_snapshot_id}")
    print(f"[END SNAPSHOT ID]\n{end_snapshot_id}")

    if end_snapshot_id is None:
        raise ValueError("end_snapshot_id must be provided!")

    # incremental read
    try:
        reader = ss.read.format("iceberg")

        if start_snapshot_id:
            reader = reader.option("start-snapshot-id", str(start_snapshot_id)) \
                            .option("end-snapshot-id", str(end_snapshot_id))

        bronze_df = reader.load("iceberg.bronze.webtoon_user_events_raw") \
                            .filter(col("datetime") == args.date)

        raw_count = bronze_df.count()
        print(f"Raw count : {raw_count}")
    except Exception as e:
        print(f"[WARN] Snapshot lineage mismatch detected : {e}")
        bronze_df = ss.read.table("iceberg.bronze.webtoon_user_events_raw") \
                        .filter(col("datetime") == args.date)

        raw_count = bronze_df.count()
        print(f"Raw count : {raw_count}")

    # transformation
    bronze_df = bronze_df \
                    .withColumn("utimestamptz", to_timestamp("utimestamptz")) \
                    .withColumn("local_timestamptz", to_timestamp("local_timestamptz"))
    
    # deduplication
    win = Window.partitionBy("event_id").orderBy(col("utimestamptz").desc())
    bronze_df = bronze_df.withColumn("row_num", row_number().over(win)) \
                                .filter("row_num = 1").drop("row_num")
    dedup_count = bronze_df.count()
    res = raw_count - dedup_count
    print(f"[Deduplication] dropped rows : {res}")
    
    # not null
    bronze_df = bronze_df.filter(col("user_id").isNotNull() &
                                 col("webtoon_id").isNotNull() &
                                 col("episode_id").isNotNull() &
                                 col("datetime").isNotNull())
    res = dedup_count - bronze_df.count()
    print(f"[Null drop] dropped rows : {res}")

    bronze_df = bronze_df.fillna({
        "platform": "UNKNOWN",
        "country": "UNKNOWN",
        "device": "UNKNOWN",
        "browser": "UNKNOWN"
    })
    
    # aggregation
    windowed_df = bronze_df.groupBy("session_id", "user_id", "webtoon_id", "episode_id",
                                    "platform", "country", "device", "browser",
                                    "datetime",
                                    window("utimestamptz", "30 seconds").alias("window")) \
                            .agg(
                                min("utimestamptz").alias("start_time"),
                                max("utimestamptz").alias("end_time"),
                                max("dwell_time_ms").alias("duration_ms"),
                                avg("scroll_ratio").alias("avg_scroll_ratio"),
                                max("scroll_ratio").alias("max_scroll_ratio"),
                                max(col("event_type") == "enter").alias("seen_enter"),
                                max(col("event_type") == "scroll").alias("seen_scroll"),
                                max(col("event_type") == "complete").alias("seen_complete"),
                                max(col("event_type") == "exit").alias("seen_exit")
                            ) \
                            .withColumn("is_complete",
                                        (col("seen_enter") & col("seen_scroll") & col("seen_complete") & (col("max_scroll_ratio") >= 1.0) & (unix_timestamp("end_time") - unix_timestamp("start_time") <= 300)).cast("int")
                            ) \
                            .withColumn("is_exit",
                                        (col("seen_enter") & col("seen_scroll") & col("seen_exit") & (col("max_scroll_ratio") < 1.0) & (unix_timestamp("end_time") - unix_timestamp("start_time") >= 600)).cast("int")
                            )
    
    windowed_df.show(20, truncate=False)

    query = windowed_df \
        .writeTo("iceberg.silver.webtoon_user_session_events") \
        .overwritePartitions()
