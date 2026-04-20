import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from config.config import HDFS_BRONZE_CHICAGO, BRONZE_CHICAGO, SILVER_CHICAGO, HDFS_SILVER_CHICAGO
from processing.transformations import transform_chicago, transform_chicago_dq
from utils.utils import log_info, log_success, log_error, print_summary, save_local_copy

def main():
    log_info("Silver Chicago Processing - Running")

    spark = SparkSession.builder \
        .appName("Silver-Chicago-Processing") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        try:
            log_info(f"Reading bronze data from HDFS: {HDFS_BRONZE_CHICAGO}")
            df = spark.read.json(f"{HDFS_BRONZE_CHICAGO}/*.json")
            df.count()
        except Exception as hdfs_err:
            log_info(f"HDFS bronze not available ({hdfs_err}), falling back to local: {BRONZE_CHICAGO}")
            if not os.path.exists(BRONZE_CHICAGO):
                log_error(f"Local bronze path also missing: {BRONZE_CHICAGO} — run ingestion first.")
                sys.exit(1)
            df = spark.read.json(f"file:///{BRONZE_CHICAGO.lstrip('/')}/*.json")

        if df.rdd.isEmpty():
            log_error("Silver Chicago: bronze layer is empty — no data to process")
            sys.exit(1)

        df = df.select(explode("data").alias("record")).select("record.*")

        initial_count = df.count()
        if initial_count == 0:
            log_error("Silver Chicago: no records after explode — exiting")
            sys.exit(1)
        log_info(f"Total raw records loaded: {initial_count}")

        df = transform_chicago_dq(df)
        df = transform_chicago(df)
        
        print_summary(df, "Silver Chicago")

        save_local_copy(df, SILVER_CHICAGO)
        
        log_info(f"Writing silver data to HDFS: {HDFS_SILVER_CHICAGO}")
        df.write.mode("overwrite").partitionBy("year", "month").parquet(HDFS_SILVER_CHICAGO)

        log_success("Silver Chicago Processing - Completed")

    except Exception as e:
        log_error(f"Silver Chicago Processing failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()