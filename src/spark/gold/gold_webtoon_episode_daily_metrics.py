import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot_date", required=False)
    args = parser.parse_args()

    snapshot_date = args.snapshot_date or datetime.now().strftime("%Y-%m-%d")
    print(f"[INFO] Snapshot date : {snapshot_date}")

    ss = SparkSession.builder \
                    .appName("GoldWebtoonEpisodeDailyMetrics") \
                    .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.webtoon_episode_daily_metrics (
            datetime DATE,
            webtoon_id STRING,
            episode_id STRING,

            total_sessions BIGINT,
            complete_sessions BIGINT,
            exit_sessions BIGINT,
            incomplete_sessions BIGINT,
            unique_users BIGINT,
            unique_complete_users BIGINT,
            unique_exit_users BIGINT,
            avg_scroll_ratio DOUBLE,
            avg_duration_ms DOUBLE,
            completion_rate DOUBLE,
            exit_rate DOUBLE,
            timeout_exit_rate DOUBLE,
            bounce_ratio DOUBLE,
            scroll_dropoff_point DOUBLE,
            scroll_bucket_0_20 BIGINT,
            scroll_bucket_20_40 BIGINT,
            scroll_bucket_40_60 BIGINT,
            scroll_bucket_60_80 BIGINT,
            scroll_bucket_80_100 BIGINT,
            continued_user_count BIGINT,
            episode_attractiveness_index DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read \
                .format("iceberg") \
                .load("iceberg.silver.webtoon_user_session_events") \
                .filter(col("datetime") == snapshot_date)

    # Daily webtoon-episode metrics
    episode_daily_df = silver_df.groupBy("datetime", "webtoon_id", "episode_id") \
                                .agg(
                                    countDistinct("session_id").alias("total_sessions"),
                                    sum("is_complete").alias("complete_sessions"),
                                    sum("is_exit").alias("exit_sessions"),
                                    sum(when(col("session_state") == "IN_PROGRESS", 1).otherwise(0)).alias("incomplete_sessions"),
                                    countDistinct("user_id").alias("unique_users"),
                                    countDistinct(when(col("is_complete") == 1, col("user_id"))).alias("unique_complete_users"),
                                    countDistinct(when(col("is_exit") == 1, col("user_id"))).alias("unique_exit_users"),
                                    round(avg(col("max_scroll_ratio")), 2).alias("avg_scroll_ratio"),
                                    round(avg(col("duration_ms").cast("double")), 2).alias("avg_duration_ms"),
                                    round(avg(col("is_complete").cast("double")), 2).alias("completion_rate"),
                                    round(avg(col("is_exit").cast("double")), 2).alias("exit_rate"),
                                    round(avg(when(col("session_state") == "TIMEOUT_EXIT", 1.0).otherwise(0.0)), 2).alias("timeout_exit_rate"),
                                    round(avg(when(col("duration_ms") <= 10000, 1.0).otherwise(0.0)), 2).alias("bounce_ratio"),
                                    coalesce(round(avg(when(col("is_exit") == 1, col("max_scroll_ratio")).otherwise(None)), 2), lit(0.0)).alias("scroll_dropoff_point"),

                                    sum(when((col("max_scroll_ratio") >= 0.0) & (col("max_scroll_ratio") < 0.2), 1).otherwise(0)).alias("scroll_bucket_0_20"),
                                    sum(when((col("max_scroll_ratio") >= 0.2) & (col("max_scroll_ratio") < 0.4), 1).otherwise(0)).alias("scroll_bucket_20_40"),
                                    sum(when((col("max_scroll_ratio") >= 0.4) & (col("max_scroll_ratio") < 0.6), 1).otherwise(0)).alias("scroll_bucket_40_60"),
                                    sum(when((col("max_scroll_ratio") >= 0.6) & (col("max_scroll_ratio") < 0.8), 1).otherwise(0)).alias("scroll_bucket_60_80"),
                                    sum(when((col("max_scroll_ratio") >= 0.8) & (col("max_scroll_ratio") <= 1.0), 1).otherwise(0)).alias("scroll_bucket_80_100")
                                )
    
    # Calculate episode_attractiveness_index
    episode_user_df = silver_df.filter(col("is_complete") == 1) \
                            .select("datetime", "webtoon_id", "episode_id", "user_id") \
                            .dropDuplicates(["datetime", "webtoon_id", "episode_id", "user_id"])

    w = Window.partitionBy("datetime", "webtoon_id", "user_id").orderBy("episode_id")

    episode_user_df = episode_user_df \
                            .withColumn("next_episode_id", lead("episode_id").over(w)) \
                            .withColumn("continued_to_next_episode",
                                        when(col("next_episode_id").isNotNull(), lit(1)).otherwise(0))

    continuation_df = episode_user_df.groupBy("datetime", "webtoon_id", "episode_id") \
                            .agg(
                                countDistinct(when(col("continued_to_next_episode") == 1, col("user_id"))).alias("continued_user_count")
                            )

    episode_daily_df = episode_daily_df.join(
                                    continuation_df,
                                    on=["datetime", "webtoon_id", "episode_id"],
                                    how="left"
    )

    episode_daily_df = episode_daily_df \
                                .withColumn("continued_user_count", coalesce(col("continued_user_count"), lit(0))) \
                                .withColumn("episode_attractiveness_index",
                                            when(col("unique_users") > 0, round(col("continued_user_count") / col("unique_users"), 2)).otherwise(lit(0.0))) \
                                .orderBy("webtoon_id", "episode_id")

    episode_daily_df.show(20, truncate=False)

    episode_daily_df.writeTo("iceberg.gold.webtoon_episode_daily_metrics").overwritePartitions()
    print("[INFO] Successfully write to gold.webtoon_episode_daily_metrics table!")
