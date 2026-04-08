from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, lit

from config.config import BRONZE_CHICAGO, SILVER_CHICAGO

spark = SparkSession.builder.appName("Silver-Chicago").getOrCreate()

# Read ALL JSON files
df = spark.read.json(BRONZE_CHICAGO)

print("Schema:")
df.printSchema()

print("Sample Data:")
df.show(5)

# CLEANING
df = df.filter(col("crash_record_id").isNotNull())

# convert date properly (Chicago API uses timestamp string)
df = df.withColumn("crash_date", to_timestamp("crash_date"))

# remove duplicates
df = df.dropDuplicates(["crash_record_id"])

# add partitions
df = df.withColumn("year", year("crash_date")) \
       .withColumn("month", month("crash_date")) \
       .withColumn("source", lit("chicago"))

# WRITE
df.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_CHICAGO)

print("Silver Chicago Completed")