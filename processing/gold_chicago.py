from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, desc
from config.config import SILVER_CHICAGO, GOLD_CHICAGO
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
        .appName("Gold-Chicago") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    print(f"[INFO] Reading Silver Chicago data from: {SILVER_CHICAGO}")
    if not os.path.exists(SILVER_CHICAGO):
        print("[WARNING] No Silver Chicago data found. Run silver_chicago.py first.")
        return

    df = spark.read.parquet(SILVER_CHICAGO)
    total = df.count()
    print(f"[INFO] Silver Chicago records loaded: {total}")

    # GOLD TABLE 1: Crash Severity Summary (by year, month, severity)

    print("\n" + "=" * 70)
    print("  GOLD REPORT 1: Crash Severity Summary")
    print("=" * 70)

    severity_df = df.groupBy("year", "month", "severity") \
        .agg(
            count("*").alias("total_crashes"),
            spark_round(avg("injuries_total"), 2).alias("avg_injuries")
        ) \
        .orderBy("year", "month", "severity")

    severity_df.show(50, truncate=False)

    severity_path = os.path.join(GOLD_CHICAGO, "crash_severity_summary")
    severity_df.write.mode("overwrite").parquet(severity_path)
    write_to_mysql(severity_df, "gold_crash_severity_summary")
    print(f"[INFO] Saved to: {severity_path} + MySQL table: gold_crash_severity_summary")

    # GOLD TABLE 2: Crash Time Analysis (by hour and day of week)

    print("\n" + "=" * 70)
    print("  GOLD REPORT 2: Crash Time Analysis")
    print("=" * 70)

    time_df = df.groupBy("crash_hour", "crash_day_of_week") \
        .agg(
            count("*").alias("total_crashes"),
            spark_round(avg("injuries_total"), 2).alias("avg_injuries"),
            spark_sum(col("injuries_fatal").cast("int")).alias("total_fatalities")
        ) \
        .orderBy(desc("total_crashes"))

    time_df.show(50, truncate=False)

    time_path = os.path.join(GOLD_CHICAGO, "crash_time_analysis")
    time_df.write.mode("overwrite").parquet(time_path)
    write_to_mysql(time_df, "gold_crash_time_analysis")
    print(f"[INFO] Saved to: {time_path} + MySQL table: gold_crash_time_analysis")

    # GOLD TABLE 3: Crash Hotspots (top streets by crash count)

    print("\n" + "=" * 70)
    print("  GOLD REPORT 3: Top 20 Crash Hotspot Streets")
    print("=" * 70)

    hotspot_df = df.filter(col("street_name").isNotNull()) \
        .groupBy("street_name") \
        .agg(
            count("*").alias("total_crashes"),
            spark_round(avg("injuries_total"), 2).alias("avg_injuries"),
            spark_sum(col("injuries_fatal").cast("int")).alias("total_fatalities")
        ) \
        .orderBy(desc("total_crashes")) \
        .limit(20)

    hotspot_df.show(20, truncate=False)

    hotspot_path = os.path.join(GOLD_CHICAGO, "crash_hotspots")
    hotspot_df.write.mode("overwrite").parquet(hotspot_path)
    write_to_mysql(hotspot_df, "gold_crash_hotspots")
    print(f"[INFO] Saved to: {hotspot_path} + MySQL table: gold_crash_hotspots")

    # GOLD TABLE 4: Top Crash Contributing Causes

    print("\n" + "=" * 70)
    print("  GOLD REPORT 4: Top 20 Crash Contributing Causes")
    print("=" * 70)

    causes_df = df.filter(col("prim_contributory_cause").isNotNull()) \
        .groupBy("prim_contributory_cause") \
        .agg(
            count("*").alias("total_crashes"),
            spark_round(avg("injuries_total"), 2).alias("avg_injuries"),
            spark_sum(col("injuries_fatal").cast("int")).alias("total_fatalities")
        ) \
        .orderBy(desc("total_crashes")) \
        .limit(20)

    causes_df.show(20, truncate=False)

    causes_path = os.path.join(GOLD_CHICAGO, "crash_causes")
    causes_df.write.mode("overwrite").parquet(causes_path)
    write_to_mysql(causes_df, "gold_crash_causes")
    print(f"[INFO] Saved to: {causes_path} + MySQL table: gold_crash_causes")

    print("\n" + "=" * 70)
    print("  GOLD CHICAGO PROCESSING COMPLETE")
    print(f"  Total source records: {total}")
    print(f"  Gold tables created: 4")
    print("=" * 70)


if __name__ == "__main__":
    process()
