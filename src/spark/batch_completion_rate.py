import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col
from datetime import datetime


SPARK_PARQUET_SILVER_PATH = os.getenv("SPARK_PARQUET_SILVER_PATH")
# SPARK_PARQUET_OUTPUT_PATH = os.getenv("SPARK_PARQUET_OUTPUT_PATH")


if __name__ == "__main__":
    ss = SparkSession.builder \
        .appName("CompletionRatePerEpisode") \
        .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    
    # df = ss.read.parquet(f"{SPARK_PARQUET_SILVER_PATH}datetime={args.date}/")
    df = ss.read.format("iceberg").load("iceberg.silver.user_session_complete_or_exit")

    result_df = df.groupBy("webtoon_id", "episode_id", "datetime").agg(
        count("*").alias("total_cnt"),
        sum("is_complete").alias("complete_cnt"),
        sum("is_exit").alias("exit_cnt")
    ) \
    .withColumn("completion_rate", (col("complete_cnt") / col("total_cnt")).cast("double")) \
    .withColumn("exit_rate", (col("exit_cnt") / col("total_cnt")).cast("double"))

    # result_df.write.mode("overwrite").parquet(f"{SPARK_PARQUET_OUTPUT_PATH}/datetime={args.date}/")
    result_df.show(20, truncate=False)

    ss.stop()