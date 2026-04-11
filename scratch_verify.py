from pyspark.sql import SparkSession
import os

def check():
    print("[INFO] Starting Spark Session with Hive Support...")
    spark = SparkSession.builder \
        .appName("Hive Validation Test") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("[INFO] Running DDL to map optimized tables...")
    spark.sql("CREATE DATABASE IF NOT EXISTS data_pipeline")
    spark.sql("USE data_pipeline")
    
    # Chicago Silver
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver_chicago_optimized (
            crash_record_id STRING,
            crash_date TIMESTAMP,
            crash_hour INT,
            crash_day_of_week INT,
            severity STRING,
            injuries_total INT,
            injuries_fatal INT,
            street_name STRING,
            prim_contributory_cause STRING,
            posted_speed_limit INT,
            weather_condition STRING,
            lighting_condition STRING,
            roadway_surface_cond STRING
        )
        PARTITIONED BY (`year` INT, `month` INT)
        STORED AS PARQUET
        LOCATION '/workspace/data/silver/chicago'
    """)
    print("[INFO] MSCK REPAIR TABLE data_pipeline.silver_chicago_optimized...")
    try:
        spark.sql("MSCK REPAIR TABLE silver_chicago_optimized")
        print("[OK] Chicago partitions recovered.")
        count = spark.sql("SELECT COUNT(*) as total FROM silver_chicago_optimized").collect()[0]["total"]
        print(f"[OK] Chicago Row Count: {count}")
    except Exception as e:
        print(f"[ERROR] {e}")

    # Youtube Silver
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver_youtube_optimized (
            video_id STRING,
            title STRING,
            channel STRING,
            published_at TIMESTAMP,
            views BIGINT,
            likes BIGINT,
            comments BIGINT,
            engagement_score DOUBLE,
            is_viral BOOLEAN
        )
        STORED AS PARQUET
        LOCATION '/workspace/data/silver/youtube'
    """)
    try:
        null_chk = spark.sql("""
            SELECT 
                SUM(CASE WHEN video_id IS NULL THEN 1 ELSE 0 END) as null_video_ids,
                SUM(CASE WHEN engagement_score IS NULL THEN 1 ELSE 0 END) as null_scores
            FROM silver_youtube_optimized
        """).collect()[0]
        print(f"[OK] YouTube Null Video IDs: {null_chk['null_video_ids']}, Null Scores: {null_chk['null_scores']}")
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    check()
