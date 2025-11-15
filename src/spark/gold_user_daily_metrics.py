import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta


SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot_date", required=False)
    args = parser.parse_args()

    snapshot_date = args.snapshot_date or datetime.now().strftime("%Y-%m-%d")
    print(f"[INFO] Snapshot date : {snapshot_date}")

    ss = SparkSession.builder \
                    .appName("GoldUserDailyMetrics") \
                    .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.user_daily_metrics (
            datetime DATE,
            user_id INT,
            total_episode_sessions BIGINT,
            complete_sessions BIGINT,
            exit_sessions BIGINT,
            incomplete_sessions BIGINT,
            avg_scroll_ratio DOUBLE,
            avg_duration_ms BIGINT,
            bounce_ratio DOUBLE,
            repeat_episode_complete_count BIGINT,
            distinct_episode_views BIGINT,
            distinct_episode_completes BIGINT,
            completion_rate DOUBLE,
            exit_rate DOUBLE,
            multi_episode_read_flag INT,
            avg_return_interval_sec DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read \
                .format("iceberg") \
                .load("iceberg.silver.webtoon_user_session_events") \
                .filter(col("datetime") == snapshot_date)

    # Daily user metrics
    user_daily_df = silver_df.groupBy("datetime", "user_id") \
                                .agg(
                                    countDistinct(struct("session_id", "episode_id")).alias("total_episode_sessions"),
                                    sum("is_complete").alias("complete_sessions"),
                                    sum("is_exit").alias("exit_sessions"),
                                    sum(when(col("session_state") == "IN_PROGRESS", 1).otherwise(0)).alias("incomplete_sessions"),
                                    round(avg("max_scroll_ratio"), 2).alias("avg_scroll_ratio"),
                                    round(avg("duration_ms"), 2).alias("avg_duration_ms"),
                                    round(avg((col("duration_ms") <= 10000).cast("double")), 2).alias("bounce_ratio"),
                                    countDistinct("episode_id").alias("distinct_episode_views"),
                                    countDistinct(when(col("is_complete") == 1, col("episode_id"))).alias("distinct_episode_completes")
                                ) \
                                .withColumn("completion_rate", round(col("complete_sessions") / col("total_episode_sessions"), 2)) \
                                .withColumn("exit_rate", round(col("exit_sessions") / col("total_episode_sessions"), 2)) \
                                .withColumn("multi_episode_read_flag", when(col("distinct_episode_views") > 1, lit(1)).otherwise(lit(0)))

    # Calculate session interval (return interval)
    w = Window.partitionBy("user_id").orderBy("start_time")

    session_interval_df = silver_df.select("datetime", "user_id", "start_time", "end_time") \
                                    .withColumn("prev_end_time", lag("end_time", 1).over(w)) \
                                    .withColumn("interval_sec", unix_timestamp("start_time") - unix_timestamp("prev_end_time")) \
                                    .groupBy("datetime", "user_id") \
                                    .agg(
                                        coalesce(round(avg("interval_sec"), 2), lit(0.0)).alias("avg_return_interval_sec")
                                    )

    user_daily_df = user_daily_df.join(
                                    session_interval_df, ["datetime", "user_id"], "left"
    )

    # Calculate total repeat count
    repeat_complete_df = silver_df.filter(col("is_complete") == 1) \
                                    .groupBy("datetime", "user_id", "episode_id") \
                                    .agg(
                                        count("*").alias("complete_count_per_episode")
                                    ) \
                                    .withColumn("repeat_count", greatest(col("complete_count_per_episode") - lit(1), lit(0))) \
                                    .groupBy("datetime", "user_id") \
                                    .agg(
                                        sum("repeat_count").alias("repeat_episode_complete_count")
                                    )

    user_daily_df = user_daily_df.join(
                                    repeat_complete_df, ["datetime", "user_id"], "left"
                                ) \
                                .na.fill({"repeat_episode_complete_count": 0})

    user_daily_df.show(20, truncate=False)

    user_daily_df.writeTo("iceberg.gold.user_daily_metrics").append()
    print("[INFO] Successfully write to gold.user_daily_metrics table!")
