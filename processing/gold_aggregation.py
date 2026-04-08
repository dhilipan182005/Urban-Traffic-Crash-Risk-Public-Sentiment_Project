from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, desc, col

from config.config import SILVER_CHICAGO, GOLD_PATH

spark = SparkSession.builder.appName("Gold").getOrCreate()

# Chicago data
df = spark.read.parquet(SILVER_CHICAGO)

# YouTube data
youtube = spark.read.parquet("/workspace/data/silver/youtube")

# 1. Crash hotspots
hotspots = df.groupBy("latitude", "longitude").agg(
    count("*").alias("crash_count"),
    avg("injuries_total").alias("avg_injuries")
)

# 2. Crash trends
trends = df.groupBy("year", "month").agg(
    count("*").alias("total_crashes")
)

# 3. Crash severity
severity = df.groupBy("crash_type").agg(
    count("*").alias("count"),
    avg("injuries_total").alias("avg_injuries")
).orderBy(desc("count"))

# 4. YouTube analytics
top_videos = youtube.orderBy(col("views").desc())

# WRITE ALL
hotspots.write.mode("overwrite").parquet(f"{GOLD_PATH}/hotspots")
trends.write.mode("overwrite").parquet(f"{GOLD_PATH}/trends")
severity.write.mode("overwrite").parquet(f"{GOLD_PATH}/severity")
top_videos.write.mode("overwrite").parquet(f"{GOLD_PATH}/top_videos")

print("GOLD LAYER COMPLETED")