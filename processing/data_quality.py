from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config.config import SILVER_CHICAGO

spark = SparkSession.builder.appName("DQ").getOrCreate()

df = spark.read.parquet(SILVER_CHICAGO)

print("Total Rows:", df.count())

# Null check
nulls = df.filter(col("crash_record_id").isNull()).count()

# Duplicate check
dupes = df.groupBy("crash_record_id").count().filter("count > 1").count()

# Geo validation
invalid_geo = df.filter(
    (col("latitude") > 90) | (col("latitude") < -90)
).count()

print("Nulls:", nulls)
print("Duplicates:", dupes)
print("Invalid Geo:", invalid_geo)