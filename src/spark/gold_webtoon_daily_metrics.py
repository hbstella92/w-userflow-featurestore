import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime


SPARK_PARQUET_WAREHOUSE = os.getenv("SPARK_PARQUET_WAREHOUSE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot_date", required=False)
    args = parser.parse_args()

    snapshot_date = args.snapshot_date or datetime.now().strftime("%Y-%m-%d")
    print(f"[INFO] Snapshot date : {snapshot_date}")

    ss = SparkSession.builder \
                    .appName("GoldWebtoonDailyMetrics") \
                    .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.webtoon_daily_metrics(
            datetime DATE,
            webtoon_id STRING,
            deepest_episode_reached STRING,
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
            binge_read_ratio DOUBLE,
            webtoon_retention_rate DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read \
                    .format("iceberg") \
                    .load("iceberg.silver.webtoon_user_session_events") \
                    .filter(col("datetime") == snapshot_date)

    webtoon_daily_df = silver_df.groupBy("datetime", "webtoon_id") \
                                .agg(
                                    countDistinct("session_id").alias("total_sessions"),
                                    sum("is_complete").alias("complete_sessions"),
                                    sum("is_exit").alias("exit_sessions"),
                                    sum(when(col("session_state") == "IN_PROGRESS", lit(1)).otherwise(lit(0))).alias("incomplete_sessions"),
                                    countDistinct("user_id").alias("unique_users"),
                                    countDistinct(when(col("is_complete") == 1, col("user_id"))).alias("unique_complete_users"),
                                    countDistinct(when(col("is_exit") == 1, col("user_id"))).alias("unique_exit_users"),
                                    round(avg("max_scroll_ratio"), 2).alias("avg_scroll_ratio"),
                                    round(avg(col("duration_ms").cast("double")), 2).alias("avg_duration_ms"),
                                    round(avg(col("is_complete").cast("double")), 2).alias("completion_rate"),
                                    round(avg(col("is_exit").cast("double")), 2).alias("exit_rate"),
                                    round(avg(when(col("session_state") == "TIMEOUT_EXIT", 1.0).otherwise(0.0)), 2).alias("timeout_exit_rate"),
                                    round(avg(when(col("duration_ms") <= 10000, 1.0).otherwise(0.0)), 2).alias("bounce_ratio"),
                                    round(avg(when(col("is_exit") == 1, col("max_scroll_ratio"))), 2).alias("scroll_dropoff_point")
                                )

    # Calculate deepest_episode_reached
    episode_daily_df = ss.read \
                        .format("iceberg") \
                        .load("iceberg.gold.webtoon_episode_daily_metrics") \
                        .filter(col("datetime") == snapshot_date)

    w = Window.partitionBy("datetime", "webtoon_id") \
            .orderBy(col("avg_scroll_ratio").desc(), "episode_id")

    deepest_ep_df = episode_daily_df \
                        .withColumn("row_num", row_number().over(w)) \
                        .filter(col("row_num") == 1) \
                        .select("datetime", "webtoon_id", col("episode_id").alias("deepest_episode_reached"))

    # Calculate binge_read_ratio
    episode_per_user = silver_df.groupBy("datetime", "webtoon_id", "user_id") \
                                .agg(
                                    countDistinct("episode_id").alias("distinct_episodes")
                                )

    binge_df = episode_per_user.groupBy("datetime", "webtoon_id") \
                            .agg(
                                countDistinct(when(col("distinct_episodes") >= 2, col("user_id"))).alias("multi_episode_users"),
                                countDistinct("user_id").alias("unique_users_daily")
                            ) \
                            .withColumn("binge_read_ratio",
                                        when(col("unique_users_daily") > 0, round(col("multi_episode_users") / col("unique_users_daily"), 2)).otherwise(lit(0.0))
                            ) \
                            .select("datetime", "webtoon_id", "binge_read_ratio")

    # Calculate webtoon_retention_rate
    prev_date = ss.sql(f"SELECT date_add('{snapshot_date}', -1) AS d").collect()[0]["d"]

    today_users = silver_df.select("webtoon_id", "user_id") \
                        .distinct() \
                        .withColumnRenamed("user_id", "today_user_id")
    
    yesterday_users = ss.read \
                        .format("iceberg") \
                        .load("iceberg.silver.webtoon_user_session_events") \
                        .filter(col("datetime") == lit(prev_date)) \
                        .select("webtoon_id", "user_id") \
                        .distinct() \
                        .withColumnRenamed("user_id", "yesterday_user_id")

    retention_df = yesterday_users.join(
                                today_users,
                                (yesterday_users.webtoon_id == today_users.webtoon_id) &
                                (yesterday_users.yesterday_user_id == today_users.today_user_id),
                                "left"
                                ) \
                                .groupBy(yesterday_users.webtoon_id.alias("webtoon_id")) \
                                .agg(
                                    countDistinct("yesterday_user_id").alias("yesterday_unique_users"),
                                    countDistinct("today_user_id").alias("returned_users")
                                ) \
                                .withColumn("webtoon_retention_rate",
                                            when(col("yesterday_unique_users") > 0, round(col("returned_users") / col("yesterday_unique_users"), 2)).otherwise(0.0)
                                ) \
                                .withColumn("datetime", lit(snapshot_date)) \
                                .select("datetime", "webtoon_id", "webtoon_retention_rate")

    # Join entire data frames
    webtoon_daily_df = webtoon_daily_df \
                        .join(deepest_ep_df, ["datetime", "webtoon_id"], "left") \
                        .join(binge_df, ["datetime", "webtoon_id"], "left") \
                        .join(retention_df, ["datetime", "webtoon_id"], "left")

    webtoon_daily_df = webtoon_daily_df.na.fill({
                                            "binge_read_ratio": 0.0,
                                            "webtoon_retention_rate": 0.0
                                        }) \
                                        .orderBy("webtoon_id")

    webtoon_daily_df.show(30, truncate=False)

    webtoon_daily_df.writeTo("iceberg.gold.webtoon_daily_metrics").append()
