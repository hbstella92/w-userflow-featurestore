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
                    .appName("GoldPlatformDeviceDailyMetrics") \
                    .getOrCreate()

    ss.sparkContext.setLogLevel("INFO")

    ss.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.platform_device_daily_metrics(
            datetime DATE,
            platform STRING,
            device STRING,
            browser STRING,

            total_sessions BIGINT,
            unique_users BIGINT,
            sessions_per_user DOUBLE,
            completion_rate DOUBLE,
            exit_rate DOUBLE,
            timeout_exit_rate DOUBLE,
            avg_scroll_ratio DOUBLE,
            avg_duration_ms DOUBLE,
            bounce_ratio DOUBLE,
            cross_browser_completion_gap DOUBLE,
            cross_device_duration_gap DOUBLE,
            cross_platform_scroll_gap DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(datetime));
    """)

    silver_df = ss.read \
                    .format("iceberg") \
                    .load("iceberg.silver.webtoon_user_session_events") \
                    .filter(col("datetime") == snapshot_date)

    base_df = silver_df.groupBy("datetime", "platform", "device", "browser") \
                    .agg(
                        countDistinct("session_id").alias("total_sessions"),
                        countDistinct("user_id").alias("unique_users"),
                        round(avg(col("is_complete").cast("double")), 2).alias("completion_rate"),
                        round(avg(col("is_exit").cast("double")), 2).alias("exit_rate"),
                        round(avg(when(col("session_state") == "TIMEOUT_EXIT", 1.0).otherwise(0.0)), 2).alias("timeout_exit_rate"),
                        round(avg("max_scroll_ratio"), 2).alias("avg_scroll_ratio"),
                        round(avg(col("duration_ms").cast("double")), 2).alias("avg_duration_ms"),
                        round(avg((col("duration_ms") <= 10000).cast("double")), 2).alias("bounce_ratio")
                    ) \
                    .withColumn("sessions_per_user",
                                when(col("unique_users") > 0, round(col("total_sessions") / col("unique_users"), 2)).otherwise(lit(0.0))
                    )
    base_df.show(30, truncate=False)

    # Calculate cross_browser_completion_gap
    w_b = Window.partitionBy("datetime", "platform", "device")

    browser_gap = base_df \
                    .withColumn("max_completion_rate", max("completion_rate").over(w_b)) \
                    .withColumn("min_completion_rate", min("completion_rate").over(w_b)) \
                    .withColumn("cross_browser_completion_gap", round(col("max_completion_rate") - col("min_completion_rate"), 2)) \
                    .select("datetime", "platform", "device", "browser", "cross_browser_completion_gap")
    browser_gap.show(30, truncate=False)

    # Calculate cross_device_duration_gap
    w_d = Window.partitionBy("datetime", "platform", "browser")

    device_gap = base_df \
                    .withColumn("max_duration_ms", max("avg_duration_ms").over(w_d)) \
                    .withColumn("min_duration_ms", min("avg_duration_ms").over(w_d)) \
                    .withColumn("cross_device_duration_gap", round(col("max_duration_ms") - col("min_duration_ms"), 2)) \
                    .select("datetime", "platform", "device", "browser", "cross_device_duration_gap")
    device_gap.show(30, truncate=False)

    # Calculate cross_platform_scroll_gap
    w_p = Window.partitionBy("datetime", "device", "browser")

    platform_gap = base_df \
                    .withColumn("max_scroll_ratio", max("avg_scroll_ratio").over(w_p)) \
                    .withColumn("min_scroll_ratio", min("avg_scroll_ratio").over(w_p)) \
                    .withColumn("cross_platform_scroll_gap", round(col("max_scroll_ratio") - col("min_scroll_ratio"), 2)) \
                    .select("datetime", "platform", "device", "browser", "cross_platform_scroll_gap")
    platform_gap.show(30, truncate=False)

    # Join all dataframes
    result_df = base_df \
                    .join(browser_gap, ["datetime", "platform", "device", "browser"], "left") \
                    .join(device_gap, ["datetime", "platform", "device", "browser"], "left") \
                    .join(platform_gap, ["datetime", "platform", "device", "browser"], "left")

    result_df.show(30, truncate=False)
    
    result_df.writeTo("iceberg.gold.platform_device_daily_metrics").append()
