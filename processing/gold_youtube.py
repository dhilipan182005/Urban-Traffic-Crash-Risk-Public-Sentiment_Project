from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, desc, to_date
from config.config import SILVER_YOUTUBE, GOLD_YOUTUBE
import os

# MySQL connection settings
MYSQL_URL = "jdbc:mysql://mysql:3306/data_pipeline"
MYSQL_USER = "root"
MYSQL_PASSWORD = "182005kavi"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def write_to_mysql(df, table_name):
    """Write a DataFrame to MySQL, selecting only flat JDBC-compatible columns."""
    mysql_columns = [c for c, t in df.dtypes if t in ("string", "int", "bigint", "long", "double", "float", "boolean", "timestamp", "date", "integer")]
    df.select(mysql_columns).write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", table_name) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", MYSQL_DRIVER) \
        .mode("overwrite") \
        .save()


def process():
    spark = SparkSession.builder \
        .appName("Gold-YouTube") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    print(f"[INFO] Reading Silver YouTube data from: {SILVER_YOUTUBE}")
    if not os.path.exists(SILVER_YOUTUBE):
        print("[WARNING] No Silver YouTube data found. Run silver_youtube.py first.")
        return

    df = spark.read.parquet(SILVER_YOUTUBE)
    total = df.count()
    print(f"[INFO] Silver YouTube records loaded: {total}")

    # GOLD TABLE 1: Channel Summary

    print("\n" + "=" * 70)
    print("  GOLD REPORT 1: Channel Summary")
    print("=" * 70)

    channel_df = df.groupBy("channel") \
        .agg(
            count("*").alias("total_videos"),
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            spark_sum("comments").alias("total_comments"),
            spark_round(avg("engagement_score"), 4).alias("avg_engagement_score")
        ) \
        .orderBy(desc("total_videos"))

    channel_df.show(50, truncate=False)

    channel_path = os.path.join(GOLD_YOUTUBE, "channel_summary")
    channel_df.write.mode("overwrite").parquet(channel_path)
    write_to_mysql(channel_df, "gold_channel_summary")
    print(f"[INFO] Saved to: {channel_path} + MySQL table: gold_channel_summary")

    # GOLD TABLE 2: Viral / Top Content

    print("\n" + "=" * 70)
    print("  GOLD REPORT 2: Viral & Top Engagement Content")
    print("=" * 70)

    # Include videos that are viral (>1M views) OR have top engagement scores
    viral_df = df.filter(
        (col("is_viral") == True) | (col("engagement_score") > 0)
    ).select(
        "video_id", "title", "channel", "views", "likes", "comments",
        "engagement_score", "is_viral"
    ).orderBy(desc("engagement_score"))

    viral_df.show(50, truncate=False)

    viral_path = os.path.join(GOLD_YOUTUBE, "viral_content")
    viral_df.write.mode("overwrite").parquet(viral_path)
    write_to_mysql(viral_df, "gold_viral_content")
    print(f"[INFO] Saved to: {viral_path} + MySQL table: gold_viral_content")

    # GOLD TABLE 3: Publishing Trends (videos by date)

    print("\n" + "=" * 70)
    print("  GOLD REPORT 3: Publishing Trends by Date")
    print("=" * 70)

    trends_df = df.withColumn("publish_date", to_date("published_at")) \
        .groupBy("publish_date") \
        .agg(
            count("*").alias("videos_published"),
            spark_sum("views").alias("total_views"),
            spark_round(avg("engagement_score"), 4).alias("avg_engagement")
        ) \
        .orderBy("publish_date")

    trends_df.show(50, truncate=False)

    trends_path = os.path.join(GOLD_YOUTUBE, "publishing_trends")
    trends_df.write.mode("overwrite").parquet(trends_path)
    write_to_mysql(trends_df, "gold_publishing_trends")
    print(f"[INFO] Saved to: {trends_path} + MySQL table: gold_publishing_trends")

    print("\n" + "=" * 70)
    print("  GOLD YOUTUBE PROCESSING COMPLETE")
    print(f"  Total source records: {total}")
    print(f"  Gold tables created: 3")
    print("=" * 70)


if __name__ == "__main__":
    process()
