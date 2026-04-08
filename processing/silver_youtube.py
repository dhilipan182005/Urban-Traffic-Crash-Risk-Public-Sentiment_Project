from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

spark = SparkSession.builder.appName("Silver-Youtube").getOrCreate()

# 🔥 Read JSON
df = spark.read.json("/workspace/data/bronze/youtube")

print("Schema:")
df.printSchema()

# 🔥 Extract items array
df = df.select(explode("items").alias("item"))

# 🔥 Extract fields
df_clean = df.select(
    col("item.id").alias("video_id"),
    col("item.snippet.title").alias("title"),
    col("item.snippet.channelTitle").alias("channel"),
    col("item.snippet.publishedAt").alias("published_at"),
    col("item.statistics.viewCount").cast("long").alias("views"),
    col("item.statistics.likeCount").cast("long").alias("likes"),
    col("item.statistics.commentCount").cast("long").alias("comments")
)

# Filter
df_clean = df_clean.filter(col("video_id").isNotNull())

# Add metadata
df_clean = df_clean.withColumn("source", lit("youtube"))

# Write
df_clean.write.mode("overwrite").parquet("/workspace/data/silver/youtube")

print("Silver YouTube Completed")