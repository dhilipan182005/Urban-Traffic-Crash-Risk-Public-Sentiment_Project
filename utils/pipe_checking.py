from pyspark.sql import SparkSession
import sys
import os
import glob

spark = SparkSession.builder \
    .appName("pipeline_verification") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def log(msg):
    print(msg)

def check_json_path(local_path, name):
    """Check a local bronze directory that contains JSON files."""
    try:
        files = glob.glob(f"{local_path}/*.json")
        if not files:
            log(f"[FAILED] {name} → No JSON files found at {local_path}")
            return False
        df = spark.read.option("multiline", "false").json(local_path)
        count = df.count()
        if count == 0:
            log(f"[FAILED] {name} → Empty dataset")
            return False
        log(f"[SUCCESS] {name} → Files: {len(files)}, Rows: {count}")
        return True
    except Exception as e:
        log(f"[FAILED] {name} → {str(e)}")
        return False

def check_path(path, name):
    try:
        df = spark.read.parquet(path)
        count = df.count()
        if count == 0:
            log(f"[FAILED] {name} → Empty dataset")
            return False
        log(f"[SUCCESS] {name} → Rows: {count}")
        return True
    except Exception as e:
        log(f"[FAILED] {name} → {str(e)}")
        return False

def check_hive(table):
    try:
        df = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}")
        count = df.collect()[0]["cnt"]
        if count == 0:
            log(f"[FAILED] Hive {table} → Empty")
            return False
        log(f"[SUCCESS] Hive {table} → Rows: {count}")
        return True
    except Exception as e:
        log(f"[FAILED] Hive {table} → {str(e)}")
        return False

def main():

    status = True

    log("[START] Pipeline Verification")

    # BRONZE — stored locally as JSON files (ingestion writes to local filesystem)
    status &= check_json_path("/workspace/data/bronze/chicago", "Bronze Chicago")
    status &= check_json_path("/workspace/data/bronze/youtube", "Bronze YouTube")

    # SILVER
    status &= check_path("hdfs://namenode:9000/user/hadoop/silver/chicago", "Silver Chicago")
    status &= check_path("hdfs://namenode:9000/user/hadoop/silver/youtube", "Silver YouTube")

    # GOLD
    status &= check_path("hdfs://namenode:9000/user/hadoop/gold/chicago", "Gold Chicago")
    status &= check_path("hdfs://namenode:9000/user/hadoop/gold/youtube", "Gold YouTube")

    # HIVE
    status &= check_hive("gold_chicago_crashes")
    status &= check_hive("gold_youtube_analytics")

    if status:
        log("[SUCCESS] FULL PIPELINE HEALTHY")
    else:
        log("[FAILED] PIPELINE HAS ERRORS")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()