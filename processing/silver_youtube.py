#!/usr/bin/env python3
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from config.config import HDFS_BRONZE_YOUTUBE, BRONZE_YOUTUBE, SILVER_YOUTUBE, HDFS_SILVER_YOUTUBE
from processing.transformations import transform_youtube, transform_youtube_dq
from utils.utils import log_info, log_success, log_error, print_summary, save_local_copy

def main():
    log_info("Silver YouTube Processing - Running")

    spark = SparkSession.builder \
        .appName("Silver-YouTube-Processing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        try:
            log_info(f"Reading bronze data from HDFS: {HDFS_BRONZE_YOUTUBE}")
            df = spark.read.json(f"{HDFS_BRONZE_YOUTUBE}/*.json")
            df.count()
        except Exception as hdfs_err:
            log_info(f"HDFS bronze not available ({hdfs_err}), falling back to local: {BRONZE_YOUTUBE}")
            if not os.path.exists(BRONZE_YOUTUBE):
                log_error(f"Local bronze path also missing: {BRONZE_YOUTUBE} — run ingestion first.")
                sys.exit(1)
            df = spark.read.json(f"file:///{BRONZE_YOUTUBE.lstrip('/')}/*.json")

        if df.rdd.isEmpty():
            log_error("Silver YouTube: bronze layer is empty — no data to process")
            sys.exit(1)

        df = df.select(explode("data").alias("item"))
        
        df = df.select(
            col("item.id").alias("video_id"),
            col("item.snippet.title").alias("title"),
            col("item.snippet.publishedAt").alias("published_at"),
            col("item.snippet.channelTitle").alias("channel"),
            col("item.statistics.viewCount").cast("long").alias("views"),
            col("item.statistics.likeCount").cast("long").alias("likes"),
            col("item.statistics.commentCount").cast("long").alias("comments")
        )
        
        df = df.na.fill(0, ["views", "likes", "comments"])

        initial_count = df.count()
        if initial_count == 0:
            log_error("Silver YouTube: no records after explode — exiting")
            sys.exit(1)
        log_info(f"Total raw records loaded: {initial_count}")

        df = transform_youtube_dq(df)
        df = transform_youtube(df)
        
        print_summary(df, "Silver YouTube")

        save_local_copy(df, SILVER_YOUTUBE)
        
        log_info(f"Writing silver data to HDFS: {HDFS_SILVER_YOUTUBE}")
        df.write.mode("overwrite").parquet(HDFS_SILVER_YOUTUBE)

        log_success("Silver YouTube Processing - Completed")

    except Exception as e:
        log_error(f"Silver YouTube Processing failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()