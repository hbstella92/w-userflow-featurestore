import os
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from datetime import datetime


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

           start_time TIMESTAMP,
           end_time TIMESTAMP,
           duration_ms BIGINT,
           max_scroll_ratio DOUBLE,

           seen_enter BOOLEAN,
           seen_scroll BOOLEAN,
           seen_complete BOOLEAN,
           seen_exit BOOLEAN,
           
           session_state STRING,
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
    reader = ss.read.format("iceberg")

    if start_snapshot_id:
        reader = reader.option("start-snapshot-id", str(start_snapshot_id)) \
                        .option("end-snapshot-id", str(end_snapshot_id))
    else:
        print("[INFO] No start_snapshot_id provided -> full read!")

    bronze_df = reader.load("iceberg.bronze.webtoon_user_events_raw") \
                        .filter(col("datetime") == to_date(lit(args.date)))

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
                                 col("datetime").isNotNull() &
                                 col("session_id").isNotNull())
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
                                    "datetime") \
                            .agg(
                                min("utimestamptz").alias("start_time"),
                                max("utimestamptz").alias("end_time"),
                                max("dwell_time_ms").alias("duration_ms"),
                                round(max("scroll_ratio"), 2).alias("max_scroll_ratio"),
                                max(when(col("event_type") == "enter", True).otherwise(False)).alias("seen_enter"),
                                max(when(col("event_type") == "scroll", True).otherwise(False)).alias("seen_scroll"),
                                max(when(col("event_type") == "complete", True).otherwise(False)).alias("seen_complete"),
                                max(when(col("event_type") == "exit", True).otherwise(False)).alias("seen_exit")
                            )

    windowed_df = windowed_df.withColumn(
                    "session_state",
                    when(col("seen_complete") &
                        (col("max_scroll_ratio") >= 0.95),
                        lit("COMPLETE")
                    ).when(
                        col("seen_exit") &
                        (col("max_scroll_ratio") < 0.95),
                        lit("EXIT")
                    ).when(
                        unix_timestamp(current_timestamp()) - unix_timestamp("end_time") > 600,
                        lit("TIMEOUT_EXIT")
                    ).otherwise(lit("IN_PROGRESS"))
                )
    windowed_df = windowed_df \
                    .withColumn("is_complete", when(col("session_state") == "COMPLETE", 1).otherwise(0)) \
                    .withColumn("is_exit", when(col("session_state").isin("EXIT", "TIMEOUT_EXIT"), 1).otherwise(0))
    
    print("[Data] Silver table ex.")
    windowed_df.show(20, truncate=False)

    # Merge upsert (idempotent)
    windowed_df.createOrReplaceTempView("staging_sessions")

    ss.sql("""
        MERGE INTO iceberg.silver.webtoon_user_session_events t
        USING staging_sessions s
        ON t.session_id = s.session_id
        AND t.user_id = s.user_id
        AND t.webtoon_id = s.webtoon_id
        AND t.episode_id = s.episode_id
        AND t.platform = s.platform
        AND t.country = s.country
        AND t.device = s.device
        AND t.browser = s.browser
        AND t.datetime = s.datetime

        WHEN MATCHED THEN UPDATE SET
            t.start_time = s.start_time,
            t.end_time = s.end_time,
            t.duration_ms = s.duration_ms,
            t.max_scroll_ratio = s.max_scroll_ratio,
            t.seen_enter = s.seen_enter,
            t.seen_scroll = s.seen_scroll,
            t.seen_complete = s.seen_complete,
            t.seen_exit = s.seen_exit,
            t.session_state = s.session_state,
            t.is_complete = s.is_complete,
            t.is_exit = s.is_exit

        WHEN NOT MATCHED THEN INSERT (
            session_id, user_id, webtoon_id, episode_id, platform, country, device, browser, datetime,
            start_time, end_time, duration_ms, max_scroll_ratio,
            seen_enter, seen_scroll, seen_complete, seen_exit,
            session_state, is_complete, is_exit
        ) VALUES (
            s.session_id, s.user_id, s.webtoon_id, s.episode_id, s.platform, s.country, s.device, s.browser, s.datetime,
            s.start_time, s.end_time, s.duration_ms, s.max_scroll_ratio,
            s.seen_enter, s.seen_scroll, s.seen_complete, s.seen_exit,
            s.session_state, s.is_complete, s.is_exit
        )
    """)
