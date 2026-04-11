from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from config.config import BRONZE_YOUTUBE, SILVER_YOUTUBE
from processing.transformations import validate_youtube, transform_youtube
import os

def process():
    spark = SparkSession.builder \
        .appName("Silver-YouTube") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()
    
    print(f"[INFO] Reading raw data from: {BRONZE_YOUTUBE}")
    if not os.path.exists(BRONZE_YOUTUBE) or not os.listdir(BRONZE_YOUTUBE):
        print("[WARNING] No data found in Bronze YouTube. Exiting.")
        return

    # Read all JSON batches
    df = spark.read.json(BRONZE_YOUTUBE)
    
    # Extract records and flatten schema
    df = df.select(explode("data").alias("item"))
    # Base columns
    select_cols = [
        col("item.id.videoId").alias("video_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.channelTitle").alias("channel"),
        col("item.snippet.publishedAt").alias("published_at")
    ]
    
    # Add statistics if available in schema (populated by updated youtube_api.py)
    item_fields = df.select("item.*").columns
    if "statistics" in item_fields:
        select_cols.extend([
            col("item.statistics.viewCount").alias("views"),
            col("item.statistics.likeCount").alias("likes"),
            col("item.statistics.commentCount").alias("comments")
        ])
        
    df = df.select(*select_cols)

    # Search results don't have statistics. If we need them, we would call videos.list.
    # For now, we will add dummy columns if they are missing to avoid breaking downstream
    for c in ["views", "likes", "comments"]:
        if c not in df.columns:
            from pyspark.sql.functions import lit
            df = df.withColumn(c, lit(0).cast("long"))

    initial_count = df.count()
    print(f"[INFO] Records ingested: {initial_count}")

    # Data Quality Validation
    df = validate_youtube(df)
    
    # Deduplication
    df = df.dropDuplicates(["video_id"])
    final_count = df.count()
    print(f"[INFO] Records after deduplication: {final_count}")

    # Transformations
    df = transform_youtube(df)

    print("\n[INFO] --- Silver YouTube Schema ---")
    df.printSchema()
    print("\n[INFO] --- Silver YouTube Sample Data ---")
    df.show(5, truncate=False)

    # Save to Silver
    print(f"[INFO] Saving cleaned data to: {SILVER_YOUTUBE}")
    df.write.mode("overwrite").parquet(SILVER_YOUTUBE)
    
    # Save to MySQL (select only flat columns — nested structs are not JDBC-compatible)
    print(f"[INFO] Saving cleaned data to MySQL table: youtube_data")
    mysql_columns = [c for c, t in df.dtypes if t in ("string", "int", "bigint", "long", "double", "float", "boolean", "timestamp", "date", "integer")]
    df.select(mysql_columns).write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/data_pipeline") \
        .option("dbtable", "youtube_data") \
        .option("user", "root") \
        .option("password", "182005kavi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()
    
    print("[INFO] Silver YouTube Processing Complete.")

if __name__ == "__main__":
    process()