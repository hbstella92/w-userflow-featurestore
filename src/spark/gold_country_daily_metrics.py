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
                    .appName("GoldCountryDailyMetrics")\
                    .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    # traffic / engagement & UX / growth & retention / gap & index metrics
    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.country_daily_metrics(
            datetime DATE,
            country STRING,

            total_sessions BIGINT,
            unique_users BIGINT,
            sessions_per_user DOUBLE,
            completion_rate DOUBLE,
            exit_rate DOUBLE,
            timeout_exit_rate DOUBLE,
            avg_scroll_ratio DOUBLE,
            avg_duration_ms DOUBLE,
            bounce_ratio DOUBLE,

            active_user_depth DOUBLE,
            returning_users BIGINT,
            return_user_ratio DOUBLE,
            new_users BIGINT,
            new_user_ratio DOUBLE,
            completion_rate_gap_vs_global DOUBLE,
            scroll_depth_index DOUBLE,
            avg_duration_index DOUBLE,
            completion_exit_ratio DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read \
                .format("iceberg") \
                .load("iceberg.silver.webtoon_user_session_events") \
                .filter(col("datetime") == snapshot_date)

    # Country base aggregation
    country_df = silver_df.groupBy("datetime", "country") \
                        .agg(
                            countDistinct("session_id").alias("total_sessions"),
                            countDistinct("user_id").alias("unique_users"),
                            countDistinct(struct("user_id", "webtoon_id", "episode_id")).alias("total_episode_views"),
                            sum(col("is_complete")).alias("complete_sessions"),
                            round(avg(col("is_complete").cast("double")), 2).alias("completion_rate"),
                            sum(col("is_exit")).alias("exit_sessions"),
                            round(avg(col("is_exit").cast("double")), 2).alias("exit_rate"),
                            round(avg(when(col("session_state") == "TIMEOUT_EXIT", 1.0).otherwise(0.0))).alias("timeout_exit_rate"),
                            round(avg("max_scroll_ratio"), 2).alias("avg_scroll_ratio"),
                            round(avg(col("duration_ms").cast("double")), 2).alias("avg_duration_ms"),
                            round(avg(when(col("duration_ms") <= 10000, 1.0).otherwise(0.0))).alias("bounce_ratio")
                        ) \
                        .withColumn("sessions_per_user",
                                        when(col("unique_users") > 0, round(col("total_sessions") / col("unique_users"), 2)).otherwise(lit(0.0))) \
                        .withColumn("active_user_depth",
                                        when(col("unique_users") > 0, round(col("total_episode_views") / col("unique_users"), 2)).otherwise(lit(0.0)))
    country_df.show(30, truncate=False)

    # Calculate gap & index against global metrics
    global_df = country_df.groupBy("datetime") \
                        .agg(
                            round(sum("complete_sessions") / sum("total_sessions"), 2).alias("global_completion_rate"),
                            round(sum(col("avg_scroll_ratio") * col("total_sessions")) / sum("total_sessions"), 2).alias("global_avg_scroll_ratio"),
                            round(sum(col("avg_duration_ms") * col("total_sessions")) / sum("total_sessions"), 2).alias("global_avg_duration_ms")
                        )
    global_df.show(30, truncate=False)

    country_gap_df = country_df.join(global_df, ["datetime"], "left") \
                                .withColumn("completion_rate_gap_vs_global",
                                            round(col("completion_rate") - col("global_completion_rate"), 2)) \
                                .withColumn("scroll_depth_index",
                                            when(col("global_avg_scroll_ratio") > 0, round(col("avg_scroll_ratio") / col("global_avg_scroll_ratio"), 2)).otherwise(lit(0.0))) \
                                .withColumn("avg_duration_index",
                                            when(col("global_avg_duration_ms") > 0, round(col("avg_duration_ms") / col("global_avg_duration_ms"), 2)).otherwise(lit(0.0))) \
                                .withColumn("completion_exit_ratio",
                                            when(col("exit_rate") > 0, round(col("completion_rate") / col("exit_rate"), 2)).otherwise(lit(0.0)))
    country_gap_df.show(30, truncate=False)

    # Calculate retention metrics
    prev_date = ss.sql(f"SELECT date_add('{snapshot_date}', -1) AS d").collect()[0]["d"]

    today_users = silver_df.select("country", "user_id").distinct()

    silver_yesterday_df = ss.read \
                            .format("iceberg") \
                            .load("iceberg.silver.webtoon_user_session_events") \
                            .filter(col("datetime") == lit(prev_date))
    yesterday_users = silver_yesterday_df.select("country", "user_id").distinct()

    retention_join_df = today_users.alias("t") \
                            .join(yesterday_users.alias("y"),
                                    (col("t.country") == col("y.country")) & (col("t.user_id") == col("y.user_id")),
                                    "left")
    retention_join_df.show(20, truncate=False)

    retention_df = retention_join_df.groupBy(col("t.country").alias("country")) \
                                    .agg(
                                        countDistinct(col("t.user_id")).alias("today_unique_users"),
                                        countDistinct(when(col("y.user_id").isNotNull(), col("y.user_id"))).alias("returning_users")
                                    ) \
                                    .withColumn("return_user_ratio",
                                                when(col("today_unique_users") > 0, round(col("returning_users") / col("today_unique_users"), 2)).otherwise(lit(0.0))) \
                                    .withColumn("new_users",
                                                col("today_unique_users") - col("returning_users")) \
                                    .withColumn("new_user_ratio",
                                                when(col("today_unique_users") > 0, round(col("new_users") / col("today_unique_users"), 2)).otherwise(lit(0.0))) \
                                    .withColumn("datetime", lit(snapshot_date)) \
                                    .select("datetime", "country", "today_unique_users", "returning_users", "new_users", "return_user_ratio", "new_user_ratio")
    retention_df.show(30, truncate=False)

    # Join entire data frames
    country_daily_df = country_gap_df.join(retention_df, ["datetime", "country"], "left")
    country_daily_df = country_daily_df.na.fill({
        "return_user_ratio": 0.0,
        "new_user_ratio": 0.0
    })

    # Results
    country_daily_df = country_daily_df.select(
        "datetime",
        "country",
        "total_sessions",
        "unique_users",
        "sessions_per_user",
        "completion_rate",
        "exit_rate",
        "timeout_exit_rate",
        "avg_scroll_ratio",
        "avg_duration_ms",
        "bounce_ratio",
        "active_user_depth",
        "returning_users",
        "return_user_ratio",
        "new_users",
        "new_user_ratio",
        "completion_rate_gap_vs_global",
        "scroll_depth_index",
        "avg_duration_index",
        "completion_exit_ratio"
    )
    country_daily_df.show(30, truncate=False)

    country_daily_df.writeTo("iceberg.gold.country_daily_metrics").overwritePartitions()
