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
    print("[LOG] snapshot date :", snapshot_date)

    ss = SparkSession.builder \
        .appName("GoldCompletionRatePerEpisode") \
        .getOrCreate()
    
    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.webtoon_episode_level_daily_metrics (
           webtoon_id STRING,
           episode_id STRING,
           country STRING,
           datetime DATE,

           total_sessions BIGINT,
           completed_sessions BIGINT,
           exited_sessions BIGINT,
           in_progress_sessions BIGINT,
           timeout_expired_sessions BIGINT,

           completion_rate DOUBLE,
           exit_rate DOUBLE,
           timeout_exit_rate DOUBLE,
           avg_duration_ms DOUBLE,

           completion_trend_7d_slope DOUBLE,
           completion_volatility_7d DOUBLE,
           avg_user_overlap DOUBLE,

           snapshot_date DATE
        )
        USING iceberg
        PARTITIONED BY(days(datetime));
    """)
    
    # load silver file
    silver_df = ss.table("iceberg.silver.webtoon_user_session_events")
    snapshot_date_col = to_date(lit(snapshot_date))

    df_filtered = silver_df.filter(
        (col("datetime") <= snapshot_date_col) &
        (col("datetime") >= date_sub(snapshot_date_col, 6))
    )
    print(f"SILVER DF COUNT : {silver_df.count()}")
    print(f"DF FILTERED : {df_filtered.count()}")

    # Daily metrics
    daily_df = df_filtered.groupBy("webtoon_id", "episode_id", "country", "datetime") \
                            .agg(
                                count("*").alias("total_sessions"),
                                sum(when(col("session_state") == "COMPLETE", 1).otherwise(0)).alias("completed_sessions"),
                                sum(when(col("session_state") == "EXIT", 1).otherwise(0)).alias("exited_sessions"),
                                sum(when(col("session_state") == "IN_PROGRESS", 1).otherwise(0)).alias("in_progress_sessions"),
                                sum(when(col("session_state") == "TIMEOUT_EXIT", 1).otherwise(0)).alias("timeout_expired_sessions"),
                                round(avg("duration_ms"), 2).alias("avg_duration_ms"),
                            ) \
                            .withColumn("completion_rate", round(col("completed_sessions") / col("total_sessions"), 2)) \
                            .withColumn("exit_rate", round(col("exited_sessions") / col("total_sessions"), 2)) \
                            .withColumn("timeout_exit_rate", round(col("timeout_expired_sessions") / col("total_sessions"), 2))
    
    print("[DAILY DF]")
    daily_df.show(20, truncate=False)
    
    # Rolling 7-day metrics
    # w = Window.partitionBy("webtoon_id", "episode_id", "country") \
    #             .orderBy("datetime") \
    #             .rowsBetween(-6, 0)
    
    # daily_df = daily_df \
    #             .withColumn("completion_volatility_7d", stddev("completion_rate").over(w)) \
    #             .withColumn("completion_trend_7d_slope", col("completion_rate") - avg("completion_rate").over(w))
    
    # print("[ROLLING DAILY DF]")
    # daily_df.show(20, truncate=False)
    
    # User overlap
    user_df = silver_df.select("datetime", "webtoon_id", "episode_id", "user_id", "country")
    daily_users = user_df.groupBy("webtoon_id", "episode_id", "country", "datetime") \
                            .agg(
                                collect_set("user_id").alias("user_set")
                            )
    
    lag_window = Window.partitionBy("webtoon_id", "episode_id", "country").orderBy("datetime")
    user_overlap_df = daily_users \
                        .withColumn("prev_user_set", lag("user_set").over(lag_window)) \
                        .withColumn("intersection_count",
                                    when(col("prev_user_set").isNotNull(),
                                         size(array_intersect(col("user_set"), col("prev_user_set")))).otherwise(lit(0))) \
                        .withColumn("avg_user_overlap",
                                    when(size(col("user_set")) > 0,
                                         round(col("intersection_count") / size(col("user_set")), 2)).otherwise(lit(0.0))) \
                        .select("webtoon_id", "episode_id", "country", "datetime", "avg_user_overlap")
    
    print("[USER OVERLAP]")
    user_overlap_df.show(20, truncate=False)
    
    # Merge all features
    gold_df = daily_df.join(user_overlap_df, ["webtoon_id", "episode_id", "country", "datetime"], "left") \
                        .withColumn("completion_trend_7d_slope", lit(None).cast("double")) \
                        .withColumn("completion_volatility_7d", lit(None).cast("double")) \
                        .withColumn("snapshot_date", snapshot_date_col)
    
    print("[GOLD DATAFRAME]")
    gold_df.show(20, truncate=False)
    
    gold_df.writeTo("iceberg.gold.webtoon_episode_level_daily_metrics") \
                .append()
