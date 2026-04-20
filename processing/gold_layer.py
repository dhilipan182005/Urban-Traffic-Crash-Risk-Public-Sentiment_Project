import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, when, desc, round
from config.config import (
    SILVER_YOUTUBE, SILVER_CHICAGO,
    GOLD_YOUTUBE, GOLD_CHICAGO,
    HDFS_GOLD_YOUTUBE, HDFS_GOLD_CHICAGO
)
from utils.utils import log_info, log_success, log_error, print_summary, save_local_copy

MYSQL_URL  = "jdbc:mysql://mysql:3306/data_pipeline?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_OPTS = {
    "driver":   "com.mysql.cj.jdbc.Driver",
    "user":     "root",
    "password": "182005kavi",
}
JDBC_JAR   = "/opt/spark/jars/mysql-connector-j-8.0.33.jar"


def _write_mysql(df, table):
    try:
        df.write.format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", table) \
            .option("driver", MYSQL_OPTS["driver"]) \
            .option("user",   MYSQL_OPTS["user"]) \
            .option("password", MYSQL_OPTS["password"]) \
            .mode("overwrite") \
            .save()
        log_success(f"MySQL write complete: {table}")
    except Exception as e:
        log_error(f"MySQL write failed for {table}: {e}")


def process_youtube_gold(spark):
    if not os.path.exists(SILVER_YOUTUBE):
        log_error(f"Silver YouTube path missing: {SILVER_YOUTUBE}")
        return

    log_info(f"Reading YouTube silver data from {SILVER_YOUTUBE}")
    df = spark.read.parquet(f"file:///{SILVER_YOUTUBE.lstrip('/')}")

    if df.rdd.isEmpty():
        log_error("Gold YouTube: silver data is empty — skipping")
        return

    channel_stats = df.filter(col("channel").isNotNull()) \
        .groupBy("channel") \
        .agg(
            sum("views").alias("total_views"),
            avg("engagement_score").alias("avg_engagement"),
            count(when(col("is_viral") == True, 1)).alias("viral_video_count"),
            count("*").alias("video_count")
        ).orderBy(desc("video_count"))

    top_videos = df.select("video_id", "title", "engagement_score", "is_viral") \
                   .orderBy(desc("engagement_score"))

    print_summary(channel_stats, "YouTube Channel Stats")
    print_summary(top_videos, "YouTube Top Videos")

    save_local_copy(channel_stats, f"{GOLD_YOUTUBE}/channel_stats")
    save_local_copy(top_videos, f"{GOLD_YOUTUBE}/top_videos")

    log_info(f"Saving YouTube Gold to HDFS: {HDFS_GOLD_YOUTUBE}")
    channel_stats.write.mode("overwrite").parquet(HDFS_GOLD_YOUTUBE)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_youtube_analytics
        USING PARQUET
        LOCATION '{HDFS_GOLD_YOUTUBE}'
    """)
    log_success("Hive table registered: gold_youtube_analytics")

    _write_mysql(channel_stats, "youtube_channel_gold")
    _write_mysql(top_videos,    "youtube_top_videos")


def process_chicago_gold(spark):
    if not os.path.exists(SILVER_CHICAGO):
        log_error(f"Silver Chicago path missing: {SILVER_CHICAGO}")
        return

    log_info(f"Reading Chicago silver data from {SILVER_CHICAGO}")
    df = spark.read.parquet(f"file:///{SILVER_CHICAGO.lstrip('/')}")

    if df.rdd.isEmpty():
        log_error("Gold Chicago: silver data is empty — skipping")
        return

    for required_col in ("year", "month", "location", "time_of_day", "severity", "injuries_total"):
        if required_col not in df.columns:
            log_error(f"Gold Chicago: required column '{required_col}' missing in silver data")
            return

    risk_profile = df.filter(
        col("location").isNotNull() &
        col("time_of_day").isNotNull() &
        col("severity").isNotNull()
    ).groupBy("location", "time_of_day", "severity") \
     .agg(
        count("*").alias("total_crashes"),
        sum("injuries_total").alias("total_injuries")
    )

    monthly_trends = df.groupBy("year", "month").agg(
        count("*").alias("total_crashes"),
        sum(round(col("injuries_total"), 0)).alias("total_injuries")
    ).orderBy("year", "month")

    print_summary(risk_profile, "Chicago Location Risk")
    print_summary(monthly_trends, "Chicago Monthly Trends")

    save_local_copy(risk_profile, f"{GOLD_CHICAGO}/location_risk")
    save_local_copy(monthly_trends, f"{GOLD_CHICAGO}/monthly_trends")

    log_info(f"Saving Chicago Gold to HDFS: {HDFS_GOLD_CHICAGO}")
    risk_profile.write.mode("overwrite").parquet(HDFS_GOLD_CHICAGO)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_chicago_crashes
        USING PARQUET
        LOCATION '{HDFS_GOLD_CHICAGO}'
    """)
    log_success("Hive table registered: gold_chicago_crashes")

    from pyspark.sql.functions import to_json
    risk_profile_mysql = risk_profile
    for field in risk_profile.schema.fields:
        if str(field.dataType).startswith("StructType") or str(field.dataType).startswith("ArrayType"):
            risk_profile_mysql = risk_profile_mysql.withColumn(field.name, to_json(col(field.name)))

    _write_mysql(risk_profile_mysql, "chicago_risk_gold")
    _write_mysql(monthly_trends,     "chicago_monthly_trends")


def main():
    log_info("Gold Layer Processing - Running")

    spark = SparkSession.builder \
        .appName("Gold-Layer-Processing") \
        .config("spark.jars", JDBC_JAR) \
        .config("spark.driver.extraClassPath", JDBC_JAR) \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        process_youtube_gold(spark)
        process_chicago_gold(spark)
        log_success("Gold Layer Processing - Completed")
    except Exception as e:
        log_error(f"Gold Layer Processing failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
