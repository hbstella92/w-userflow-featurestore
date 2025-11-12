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

    ss = SparkSession.builder
        .appName("GoldUserDailyMetrics")
        .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.user_daily_metrics (
            datetime DATE,
            user_id INT,
            total_sessions BIGINT,
            complete_sessions BIGINT,
            exit_sessions BIGINT,
            avg_scroll_ratio DOUBLE,
            avg_duration_ms BIGINT,
            bounce_ratio DOUBLE,
            distinct_episodes BIGINT,
            completion_rate DOUBLE,
            exit_rate DOUBLE,
            multi_episode_flag DOUBLE,
            return_interval_avg DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read
        .format("iceberg")
        .load("iceberg.silver.webtoon_user_session_events")
        .filter(F.col("datetime") == snapshot_date)

    # Daily user metrics
    user_daily_df = silver_df.groupBy("datetime", "user_id")
        .agg(
            F.countDistinct("session_id").alias("total_sessions"),
            F.sum("is_complete").alias("complete_sessions"),
            F.sum("is_exit").alias("exit_sessions"),
            F.avg("max_scroll_ratio").alias("avg_scroll_ratio"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.avg((F.col("duration_ms") <= 10000).cast("double")).alias("bounce_ratio"),
            F.countDistinct("episode_id").alias("distinct_episodes")
        )
        .withColumn("completion_rate", F.col("complete_sessions") / F.col("total_sessions"))
        .withColumn("exit_rate", F.col("exit_sessions") / F.col("total_sessions"))
        .withColumn("multi_episode_flag", F.when(F.col("distinct_episodes") > 1, 1.0).otherwise(0.0))

    # Calculate session interval (return interval)
    w_session = Window.partitionBy("user_id").orderBy("start_time")

    session_interval_df = silver_df.select("datetime", "user_id", "start_time")
        .withColumn("prev_time", F.lag("start_time", 1).over(w_session))
        .withColumn("interval_sec", F.unix_timestamp("start_time") - F.unix_timestamp("prev_time"))
        .groupBy("datetime", "user_id")
        .agg(
            F.avg("interval_sec").alias("return_interval_avg")
        )

    user_daily_df = user_daily_df.join(
        session_interval_df, ["datetime", "user_id"], "left"
    )

    user_daily_df.writeTo("iceberg.gold.user_daily_metrics").append()
    print("[INFO] Successfully write to gold.user_daily_metrics table!")
