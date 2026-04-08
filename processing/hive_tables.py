from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Silver table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS silver_chicago
USING PARQUET
LOCATION '/workspace/data/silver/chicago'
""")

# Gold table
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gold_hotspots
USING PARQUET
LOCATION '/workspace/data/gold/hotspots'
""")

print("Hive Tables Created")